package store

import (
	"errors"
	"net"
	"testing"
	"time"

	"statusd/internal/model"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func openSingle(t *testing.T, dir, addr string) *Store {
	t.Helper()
	s, err := Open(Config{NodeID: "n1", DataDir: dir, Bind: addr, Bootstrap: true,
		HeartbeatTimeout: 200 * time.Millisecond, ElectionTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !s.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("never became leader")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return s
}

func mustApply(t *testing.T, s *Store, typ string, ts int64, actor string, data any) {
	t.Helper()
	c, err := NewCommand(typ, ts, actor, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(c, 5*time.Second); err != nil {
		t.Fatalf("%s: %v", typ, err)
	}
}

func tick(id string, st model.Status, l model.Level) RecordTick {
	return RecordTick{Interval: 60, Components: []TickComponent{{ID: id, Raw: l, Auto: l, AutoStatus: st, Automated: true}}}
}

func TestStoreLifecycle(t *testing.T) {
	dir := t.TempDir()
	addr := freePort(t)
	s := openSingle(t, dir, addr)

	t0 := int64(1_800_000_000)
	mustApply(t, s, CmdRecordTick, t0, "", tick("g/a", model.Operational, model.OK))
	mustApply(t, s, CmdRecordTick, t0+60, "", tick("g/a", model.MajorOutage, model.Down))

	// Override wins and is audited.
	mustApply(t, s, CmdSetOverride, t0+70, "alice", SetOverride{Component: "g/a", Status: model.DegradedPerformance, Reason: "known issue", Expires: t0 + 600})
	comps, _ := s.Components()
	if comps["g/a"].Override == nil || comps["g/a"].AutoStatus != model.MajorOutage {
		t.Fatalf("override/auto not stored: %+v", comps["g/a"])
	}

	// Validation errors come back from the FSM.
	c, _ := NewCommand(CmdSetOverride, t0+80, "alice", SetOverride{Component: "g/a", Status: "bogus", Reason: "x"})
	if err := s.Apply(c, time.Second); !errors.Is(err, ErrValidation) {
		t.Fatalf("expected validation error, got %v", err)
	}

	// Incident with first update, then resolve.
	mustApply(t, s, CmdUpsertIncident, t0+90, "alice", UpsertIncident{ID: "inc1", Title: "Outage", Impact: "major", Components: []string{"g/a"},
		FirstUpdate: &IncidentUpdate{ID: "u1", State: model.IncidentInvestigating, Body: "Looking"}})
	mustApply(t, s, CmdAddIncidentUpdate, t0+200, "bob", AddIncidentUpdate{IncidentUpdate{ID: "u2", IncidentID: "inc1", State: model.IncidentResolved, Body: "Fixed"}})
	inc, err := s.Incident("inc1")
	if err != nil || inc.State != model.IncidentResolved || inc.Resolved != t0+200 || len(inc.Updates) != 2 {
		t.Fatalf("incident: %+v %v", inc, err)
	}

	// Maintenance makes the component under_maintenance (when not overridden).
	mustApply(t, s, CmdClearOverride, t0+210, "alice", ClearOverride{Component: "g/a"})
	mustApply(t, s, CmdUpsertMaintenance, t0+220, "alice", Maintenance{ID: "m1", Title: "Work", Starts: t0 + 300, Ends: t0 + 900, Components: []string{"g/a"}})
	mustApply(t, s, CmdRecordTick, t0+360, "", tick("g/a", model.MajorOutage, model.Down))
	changes, _ := s.StatusChanges("g/a", 0, 100)
	want := []string{"under_maintenance", "major_outage", "degraded_performance", "major_outage", "operational"}
	if len(changes) != len(want) {
		t.Fatalf("changes: %+v", changes)
	}
	for i, c := range changes {
		if c.Status != want[i] {
			t.Fatalf("change %d: got %s want %s (%+v)", i, c.Status, want[i], changes)
		}
	}

	// Paused automation freezes the automatic status.
	mustApply(t, s, CmdSetSetting, t0+400, "root", SetSetting{Key: SettingAutomationPaused, Value: "1"})
	mustApply(t, s, CmdRecordTick, t0+460, "", tick("g/a", model.Operational, model.OK))
	comps, _ = s.Components()
	if comps["g/a"].AutoStatus != model.MajorOutage {
		t.Fatalf("paused automation should keep auto status, got %s", comps["g/a"].AutoStatus)
	}

	// Daily accounting.
	daily, _ := s.Daily("2000-01-01")
	var total float64
	for _, d := range daily["g/a"] {
		for _, v := range d {
			total += v
		}
	}
	if total != 240 {
		t.Fatalf("daily seconds: %v (%v)", total, daily)
	}

	audit, _ := s.Audit(100)
	if len(audit) < 7 {
		t.Fatalf("audit entries: %d", len(audit))
	}

	// Snapshot, restart: state is rebuilt from snapshot + log.
	if err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, CmdUpsertAnnouncement, t0+500, "alice", Announcement{ID: "a1", Level: "info", Title: "Hello", Starts: t0})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openSingle(t, dir, addr)
	defer s2.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		anns, _ := s2.Announcements()
		inc2, _ := s2.Incident("inc1")
		if len(anns) == 1 && inc2 != nil && len(inc2.Updates) == 2 && s2.LastTick() == t0+460 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state not restored: anns=%d inc=%v lastTick=%d", len(anns), inc2, s2.LastTick())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestNotLeader(t *testing.T) {
	// A node configured with peers that don't exist never wins an election.
	s, err := Open(Config{NodeID: "a", DataDir: t.TempDir(), Bind: freePort(t), Bootstrap: true,
		Peers: []Peer{{ID: "a", Address: "127.0.0.1:1"}, {ID: "b", Address: "127.0.0.1:2"}, {ID: "c", Address: "127.0.0.1:3"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, _ := NewCommand(CmdSetSetting, 1, "x", SetSetting{Key: "k", Value: "v"})
	if err := s.Apply(c, time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

func TestWebhookStateSurvivesSnapshot(t *testing.T) {
	dir := t.TempDir()
	addr := freePort(t)
	s := openSingle(t, dir, addr)
	t0 := int64(1_800_000_000)
	mustApply(t, s, CmdWebhookDelivered, t0, "", WebhookDelivered{Webhook: "ops", Statuses: map[string]model.Status{"g/a": model.Operational, "g/b": model.Operational}})
	if err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	mustApply(t, s, CmdWebhookDelivered, t0+60, "", WebhookDelivered{Webhook: "ops", Statuses: map[string]model.Status{"g/a": model.MajorOutage}})
	c, _ := NewCommand(CmdWebhookDelivered, t0+70, "", WebhookDelivered{Webhook: "ops", Statuses: map[string]model.Status{"g/a": "bogus"}})
	if err := s.Apply(c, 5*time.Second); err == nil {
		t.Fatal("bad status accepted")
	}
	if a, _ := s.Audit(10); len(a) != 0 {
		t.Fatalf("webhook bookkeeping must not be audited: %v", a)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := openSingle(t, dir, addr)
	defer s2.Close()
	want := map[string]model.Status{"g/a": model.MajorOutage, "g/b": model.Operational}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := s2.WebhookState("ops")
		if len(got) == 2 && got["g/a"] == want["g/a"] && got["g/b"] == want["g/b"] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("webhook state not restored: %v", got)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if other, _ := s2.WebhookState("chat"); len(other) != 0 {
		t.Fatalf("state leaked across webhooks: %v", other)
	}
}
