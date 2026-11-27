package webhook

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/model"
	"statusd/internal/view"
)

// memStore is the replicated state, shared between "leaders" in failover tests.
type memStore struct {
	mu     sync.Mutex
	leader bool
	state  map[string]map[string]model.Status
	fail   bool // Record fails (leadership lost)
}

func (m *memStore) IsLeader() bool              { return m.leader }
func (m *memStore) Barrier(time.Duration) error { return nil }
func (m *memStore) WebhookState(h string) (map[string]model.Status, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]model.Status{}
	for k, v := range m.state[h] {
		out[k] = v
	}
	return out, nil
}
func (m *memStore) Record(h string, st map[string]model.Status) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return io.ErrClosedPipe
	}
	if m.state == nil {
		m.state = map[string]map[string]model.Status{}
	}
	if m.state[h] == nil {
		m.state[h] = map[string]model.Status{}
	}
	for k, v := range st {
		m.state[h][k] = v
	}
	return nil
}

type receiver struct {
	mu     sync.Mutex
	code   int
	bodies [][]byte
	hdrs   []http.Header
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	b, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bodies = append(r.bodies, b)
	r.hdrs = append(r.hdrs, req.Header.Clone())
	if r.code != 0 {
		w.WriteHeader(r.code)
	}
}

func (r *receiver) n() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.bodies) }

func (r *receiver) payload(t *testing.T, i int) Payload {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var p Payload
	if err := json.Unmarshal(r.bodies[i], &p); err != nil {
		t.Fatal(err)
	}
	return p
}

type statuses struct {
	mu sync.Mutex
	m  map[string]model.Status
}

func (s *statuses) set(id string, st model.Status) { s.mu.Lock(); s.m[id] = st; s.mu.Unlock() }
func (s *statuses) resolve(time.Time) (map[string]view.ComponentStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]view.ComponentStatus{}
	for id, st := range s.m {
		out[id] = view.ComponentStatus{Status: st, Source: model.SourceAuto, Since: 1700000000}
	}
	return out, nil
}

func testCat(t *testing.T) *catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "10.yml"), []byte(`group: {slug: api, name: "API", order: 10}
components:
  - {slug: public, name: "Public API"}
  - {slug: auth, name: "Authentication"}
  - {slug: internal, name: "Internal", hidden: true}
`), 0o644)
	cat, err := catalog.Load(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

func setup(t *testing.T, hooks []Config) (*Dispatcher, *memStore, *statuses, *receiver) {
	t.Helper()
	rcv := &receiver{}
	srv := httptest.NewServer(rcv)
	t.Cleanup(srv.Close)
	for i := range hooks {
		if hooks[i].URL == "" {
			hooks[i].URL = srv.URL
		}
	}
	loaded, err := Load(hooks, func(s string) bool { return model.Status(s).Valid() })
	if err != nil {
		t.Fatal(err)
	}
	st := &statuses{m: map[string]model.Status{"api/public": model.Operational, "api/auth": model.Operational, "api/internal": model.Operational}}
	ms := &memStore{leader: true}
	d := NewDispatcher(loaded, testCat(t), st.resolve, ms, PageRef{Name: "Example", URL: "https://status.example.com"}, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	return d, ms, st, rcv
}

func TestBaselineThenDeliver(t *testing.T) {
	d, ms, st, rcv := setup(t, []Config{{Name: "ops"}})
	ctx := context.Background()
	d.Sync(ctx)
	if rcv.n() != 0 {
		t.Fatal("the first sync must only record a baseline")
	}
	if ms.state["ops"]["api/public"] != model.Operational || ms.state["ops"]["api/internal"] != "" {
		t.Fatalf("baseline: %v (hidden components are never tracked)", ms.state["ops"])
	}
	st.set("api/public", model.MajorOutage)
	st.set("api/internal", model.MajorOutage) // hidden: never sent
	d.Sync(ctx)
	d.Sync(ctx) // nothing new
	if rcv.n() != 1 {
		t.Fatalf("deliveries: %d", rcv.n())
	}
	p := rcv.payload(t, 0)
	if p.Type != "component.status_changed" || len(p.Changes) != 1 || p.Changes[0].From != model.Operational ||
		p.Changes[0].To != model.MajorOutage || p.Changes[0].Component.Name != "Public API" || p.Overall.Status != model.MajorOutage {
		t.Fatalf("payload: %+v", p)
	}
	if h := rcv.hdrs[0]; h.Get("X-Statusd-Delivery") != p.ID || h.Get("X-Statusd-Signature") != "" {
		t.Fatalf("headers: %v", h)
	}
	if ms.state["ops"]["api/public"] != model.MajorOutage {
		t.Fatal("delivery not recorded")
	}
}

func TestRetryCoalescesAndBacksOff(t *testing.T) {
	d, _, st, rcv := setup(t, []Config{{Name: "ops"}})
	ctx := context.Background()
	now := time.Unix(1800000000, 0)
	d.now = func() time.Time { return now }
	d.Sync(ctx)
	rcv.code = 500
	st.set("api/public", model.MajorOutage)
	d.Sync(ctx)
	if rcv.n() != 1 || d.Status().Hooks[0].Pending != 1 || d.Status().Hooks[0].LastError == "" {
		t.Fatalf("first failure: n=%d state=%+v", rcv.n(), d.Status().Hooks[0])
	}
	d.Sync(ctx) // inside the backoff
	if rcv.n() != 1 {
		t.Fatal("retried during backoff")
	}
	// Flaps while the endpoint is down: one from->to once it is back.
	st.set("api/public", model.PartialOutage)
	st.set("api/public", model.DegradedPerformance)
	rcv.code = 0
	now = now.Add(time.Minute)
	d.Sync(ctx)
	if rcv.n() != 2 {
		t.Fatalf("deliveries: %d", rcv.n())
	}
	c := rcv.payload(t, 1).Changes[0]
	if c.From != model.Operational || c.To != model.DegradedPerformance {
		t.Fatalf("coalesced change: %+v", c)
	}
	if h := d.Status().Hooks[0]; h.Pending != 0 || h.LastError != "" || h.LastSuccess.IsZero() {
		t.Fatalf("after recovery: %+v", h)
	}
}

func TestFailoverContinuesWithSameID(t *testing.T) {
	d1, ms, st, rcv := setup(t, []Config{{Name: "ops"}})
	ctx := context.Background()
	d1.Sync(ctx)
	st.set("api/auth", model.PartialOutage)
	ms.fail = true // delivered, then leadership lost before the commit
	d1.Sync(ctx)
	ms.fail = false
	// New leader, same replicated state: sends the same event again.
	d2 := NewDispatcher(d1.hooksCfg(), d1.cat, st.resolve, ms, d1.page, "test", d1.log)
	d2.Sync(ctx)
	d2.Sync(ctx)
	if rcv.n() != 2 {
		t.Fatalf("deliveries: %d", rcv.n())
	}
	if a, b := rcv.payload(t, 0).ID, rcv.payload(t, 1).ID; a != b {
		t.Fatalf("event ids differ: %s %s", a, b)
	}
}

func (d *Dispatcher) hooksCfg() []*Config {
	var out []*Config
	for _, h := range d.hooks {
		out = append(out, h.cfg)
	}
	return out
}

func TestFilterMinStatusAndSignature(t *testing.T) {
	sec := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(sec, []byte("s3cret\n"), 0o600)
	d, ms, st, rcv := setup(t, []Config{{Name: "auth-only", Components: "api/auth", MinStatus: "partial_outage", SecretFile: sec,
		Headers: map[string]string{"Authorization": "Bearer x"}}})
	ctx := context.Background()
	d.Sync(ctx)
	if _, ok := ms.state["auth-only"]["api/public"]; ok {
		t.Fatal("filtered component tracked")
	}
	st.set("api/public", model.MajorOutage)
	st.set("api/auth", model.DegradedPerformance) // below min_status: recorded, not sent
	d.Sync(ctx)
	if rcv.n() != 0 || ms.state["auth-only"]["api/auth"] != model.DegradedPerformance {
		t.Fatalf("n=%d state=%v", rcv.n(), ms.state["auth-only"])
	}
	st.set("api/auth", model.MajorOutage)
	d.Sync(ctx)
	if rcv.n() != 1 {
		t.Fatalf("deliveries: %d", rcv.n())
	}
	h := rcv.hdrs[0]
	if want := "sha256=" + Sign([]byte("s3cret"), h.Get("X-Statusd-Timestamp"), rcv.bodies[0]); h.Get("X-Statusd-Signature") != want {
		t.Fatalf("signature %q, want %q", h.Get("X-Statusd-Signature"), want)
	}
	if h.Get("Authorization") != "Bearer x" {
		t.Fatal("custom header missing")
	}
	// Back to degraded: from major_outage, so it is sent.
	st.set("api/auth", model.DegradedPerformance)
	d.Sync(ctx)
	if rcv.n() != 2 {
		t.Fatal("recovery from above min_status not sent")
	}
}

func TestSlackFormat(t *testing.T) {
	d, _, st, rcv := setup(t, []Config{{Name: "chat", Format: "slack"}})
	ctx := context.Background()
	d.Sync(ctx)
	st.set("api/public", model.MajorOutage)
	d.Sync(ctx)
	var m map[string]string
	json.Unmarshal(rcv.bodies[0], &m)
	if !strings.Contains(m["text"], "<https://status.example.com|Example>") || !strings.Contains(m["text"], "*Public API* (API): Operational → Major outage") {
		t.Fatalf("text: %q", m["text"])
	}
	if rcv.hdrs[0].Get("X-Statusd-Event") != "" {
		t.Fatal("slack deliveries carry no statusd headers")
	}
}

func TestNotLeaderDoesNothing(t *testing.T) {
	d, ms, st, rcv := setup(t, []Config{{Name: "ops"}})
	ms.leader = false
	d.tick(context.Background())
	st.set("api/public", model.MajorOutage)
	d.tick(context.Background())
	if rcv.n() != 0 || len(ms.state) != 0 || d.Status().Active {
		t.Fatal("a follower must not deliver or record")
	}
}

func TestLoadValidation(t *testing.T) {
	valid := func(s string) bool { return model.Status(s).Valid() }
	bad := map[string][]Config{
		"no name":      {{URL: "https://x.example"}},
		"duplicate":    {{Name: "a", URL: "https://x.example"}, {Name: "a", URL: "https://y.example"}},
		"no url":       {{Name: "a"}},
		"relative url": {{Name: "a", URL: "/hook"}},
		"bad format":   {{Name: "a", URL: "https://x.example", Format: "xml"}},
		"bad regex":    {{Name: "a", URL: "https://x.example", Components: "("}},
		"bad status":   {{Name: "a", URL: "https://x.example", MinStatus: "down"}},
		"two urls":     {{Name: "a", URL: "https://x.example", URLEnv: "X"}},
		"slack secret": {{Name: "a", URL: "https://x.example", Format: "slack", SecretEnv: "HOME"}},
	}
	for name, hooks := range bad {
		if _, err := Load(hooks, valid); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	t.Setenv("HOOK_URL", "https://hooks.example.com/T000/B000/xyz")
	hs, err := Load([]Config{{Name: "a", URLEnv: "HOOK_URL", Format: "slack"}}, valid)
	if err != nil || hs[0].url != "https://hooks.example.com/T000/B000/xyz" || hs[0].Timeout != 10*time.Second {
		t.Fatalf("url_env: %v %+v", err, hs)
	}
}

func TestErrorsDoNotLeakURL(t *testing.T) {
	d, _, st, _ := setup(t, []Config{{Name: "chat", URL: "http://127.0.0.1:1/secret-token", Format: "slack"}})
	ctx := context.Background()
	d.Sync(ctx)
	st.set("api/public", model.MajorOutage)
	d.Sync(ctx)
	if e := d.Status().Hooks[0].LastError; e == "" || strings.Contains(e, "secret-token") {
		t.Fatalf("last error %q", e)
	}
}
