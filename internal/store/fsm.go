package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	_ "modernc.org/sqlite"

	"statusd/internal/model"
)

// FSM applies raft commands to a local SQLite database.
type FSM struct {
	path string

	mu sync.RWMutex // guards db swaps during Restore
	db *sql.DB

	appliedIndex uint64
	lastTick     int64
}

func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(OFF)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version', ?)`, strconv.Itoa(schemaVersion)); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// NewFSM creates a fresh database at path. Any previous file is removed: the
// state is always rebuilt from the raft snapshot and log.
func NewFSM(path string) (*FSM, error) {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	db, err := openDB(path)
	if err != nil {
		return nil, err
	}
	return &FSM{path: path, db: db}, nil
}

func (f *FSM) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.db.Close()
}

// DB returns the database for reads. Callers must not hold it across a
// Restore; use withDB for anything longer than one query.
func (f *FSM) withDB(fn func(*sql.DB) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return fn(f.db)
}

// Apply implements raft.FSM. The returned value is nil or an error.
func (f *FSM) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand {
		return nil
	}
	var cmd Command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	tx, err := f.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if err := apply(tx, &cmd); err != nil {
		tx.Rollback()
		// Deterministic rejections (bad input) are returned to the caller;
		// every node rejects them identically, so state stays consistent.
		return err
	}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO meta(key,value) VALUES('applied_index', ?)`, strconv.FormatUint(l.Index, 10)); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	f.appliedIndex = l.Index
	if cmd.Type == CmdRecordTick {
		f.lastTick = cmd.TS
	}
	return nil
}

func apply(tx *sql.Tx, c *Command) error {
	switch c.Type {
	case CmdRecordTick:
		var d RecordTick
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		return applyTick(tx, c.TS, &d)
	case CmdSetOverride:
		var d SetOverride
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		if err := ensureComponent(tx, d.Component); err != nil {
			return err
		}
		var exp any
		if d.Expires > 0 {
			exp = d.Expires
		}
		if _, err := tx.Exec(`UPDATE components SET override_status=?, override_reason=?, override_by=?, override_set=?, override_expires=? WHERE id=?`,
			string(d.Status), d.Reason, c.Actor, c.TS, exp, d.Component); err != nil {
			return err
		}
		if err := refreshEffective(tx, d.Component, c.TS); err != nil {
			return err
		}
		return audit(tx, c, d.Component, fmt.Sprintf("status=%s expires=%d reason=%q", d.Status, d.Expires, d.Reason))
	case CmdClearOverride:
		var d ClearOverride
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := ensureComponent(tx, d.Component); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE components SET override_status=NULL, override_reason=NULL, override_by=NULL, override_set=NULL, override_expires=NULL WHERE id=?`, d.Component); err != nil {
			return err
		}
		if err := refreshEffective(tx, d.Component, c.TS); err != nil {
			return err
		}
		return audit(tx, c, d.Component, "")
	case CmdWebhookDelivered:
		var d WebhookDelivered
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		for comp, st := range d.Statuses {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO webhook_state(webhook,component,status,ts) VALUES(?,?,?,?)`,
				d.Webhook, comp, string(st), c.TS); err != nil {
				return err
			}
		}
		return nil // system bookkeeping: not audited
	case CmdSetSetting:
		var d SetSetting
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)`, d.Key, d.Value); err != nil {
			return err
		}
		return audit(tx, c, d.Key, d.Value)
	case CmdUpsertIncident:
		var d UpsertIncident
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		return applyUpsertIncident(tx, c, &d)
	case CmdAddIncidentUpdate:
		var d AddIncidentUpdate
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		return applyIncidentUpdate(tx, c, &d.IncidentUpdate)
	case CmdDeleteIncident:
		var d DeleteByID
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		for _, q := range []string{`DELETE FROM incident_updates WHERE incident_id=?`, `DELETE FROM incident_components WHERE incident_id=?`, `DELETE FROM incidents WHERE id=?`} {
			if _, err := tx.Exec(q, d.ID); err != nil {
				return err
			}
		}
		return audit(tx, c, d.ID, "")
	case CmdUpsertMaintenance:
		var d Maintenance
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		return applyUpsertMaintenance(tx, c, &d)
	case CmdDeleteMaintenance:
		var d DeleteByID
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		comps, err := maintenanceComponents(tx, d.ID)
		if err != nil {
			return err
		}
		for _, q := range []string{`DELETE FROM maintenance_components WHERE maintenance_id=?`, `DELETE FROM maintenances WHERE id=?`} {
			if _, err := tx.Exec(q, d.ID); err != nil {
				return err
			}
		}
		for _, id := range comps {
			if err := refreshEffective(tx, id, c.TS); err != nil {
				return err
			}
		}
		return audit(tx, c, d.ID, "")
	case CmdUpsertAnnouncement:
		var d Announcement
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if err := d.Validate(); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO announcements(id,level,title,body,starts,ends,created,updated,author) VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(id) DO UPDATE SET level=excluded.level, title=excluded.title, body=excluded.body, starts=excluded.starts, ends=excluded.ends, updated=excluded.updated`,
			d.ID, d.Level, d.Title, d.Body, d.Starts, d.Ends, c.TS, c.TS, c.Actor); err != nil {
			return err
		}
		return audit(tx, c, d.ID, d.Title)
	case CmdDeleteAnnouncement:
		var d DeleteByID
		if err := json.Unmarshal(c.Data, &d); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM announcements WHERE id=?`, d.ID); err != nil {
			return err
		}
		return audit(tx, c, d.ID, "")
	}
	return fmt.Errorf("unknown command %q", c.Type)
}

func audit(tx *sql.Tx, c *Command, target, detail string) error {
	actor := c.Actor
	if actor == "" {
		actor = "system"
	}
	_, err := tx.Exec(`INSERT INTO audit(ts,actor,action,target,detail) VALUES(?,?,?,?,?)`, c.TS, actor, c.Type, target, detail)
	return err
}

func ensureComponent(tx *sql.Tx, id string) error {
	_, err := tx.Exec(`INSERT OR IGNORE INTO components(id) VALUES(?)`, id)
	return err
}

func setting(tx *sql.Tx, key string) (string, error) {
	var v string
	err := tx.QueryRow(`SELECT value FROM settings WHERE key=?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func dayOf(ts int64) string { return time.Unix(ts, 0).UTC().Format("2006-01-02") }

func applyTick(tx *sql.Tx, ts int64, d *RecordTick) error {
	paused, err := setting(tx, SettingAutomationPaused)
	if err != nil {
		return err
	}
	for _, tc := range d.Components {
		if err := ensureComponent(tx, tc.ID); err != nil {
			return err
		}
		var freshTS any
		var freshLevel any
		if tc.FreshAge != nil {
			freshTS = ts - int64(*tc.FreshAge)
		}
		if tc.FreshLevel != nil {
			freshLevel = int(*tc.FreshLevel)
		}
		if _, err := tx.Exec(`UPDATE components SET raw_level=?, automated=?, signals_json=?, fresh_age=?, fresh_level=?, fresh_ts=?, last_tick=? WHERE id=?`,
			int(tc.Raw), boolInt(tc.Automated), nullJSON(tc.Signals), tc.FreshAge, freshLevel, freshTS, ts, tc.ID); err != nil {
			return err
		}
		if paused != "1" {
			if _, err := tx.Exec(`UPDATE components SET auto_since = CASE WHEN auto_status != ? OR auto_since = 0 THEN ? ELSE auto_since END,
				auto_level=?, auto_status=? WHERE id=?`, string(tc.AutoStatus), ts, int(tc.Auto), string(tc.AutoStatus), tc.ID); err != nil {
				return err
			}
		}
		if err := refreshEffective(tx, tc.ID, ts); err != nil {
			return err
		}
		if d.Interval > 0 {
			var st string
			if err := tx.QueryRow(`SELECT effective_status FROM components WHERE id=?`, tc.ID).Scan(&st); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO daily(component,day,status,seconds) VALUES(?,?,?,?)
				ON CONFLICT(component,day,status) DO UPDATE SET seconds = seconds + excluded.seconds`,
				tc.ID, dayOf(ts), st, d.Interval); err != nil {
				return err
			}
		}
	}
	if d.PruneDays > 0 {
		cutoff := ts - int64(d.PruneDays)*86400
		if _, err := tx.Exec(`DELETE FROM daily WHERE day < ?`, dayOf(cutoff)); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM status_changes WHERE ts < ? AND id NOT IN (SELECT MAX(id) FROM status_changes WHERE ts < ? GROUP BY component)`, cutoff, cutoff); err != nil {
			return err
		}
	}
	return nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// refreshEffective recomputes a component's published status at ts and
// records a status change when it differs from the previous one.
func refreshEffective(tx *sql.Tx, id string, ts int64) error {
	var (
		autoStatus, effStatus, effSource string
		automated                        int
		ovStatus, ovReason, ovBy         sql.NullString
		ovSet, ovExp                     sql.NullInt64
	)
	err := tx.QueryRow(`SELECT auto_status, automated, effective_status, effective_source, override_status, override_reason, override_by, override_set, override_expires
		FROM components WHERE id=?`, id).Scan(&autoStatus, &automated, &effStatus, &effSource, &ovStatus, &ovReason, &ovBy, &ovSet, &ovExp)
	if err != nil {
		return err
	}
	var ov *model.Override
	if ovStatus.Valid {
		ov = &model.Override{Status: model.Status(ovStatus.String), Reason: ovReason.String, By: ovBy.String, SetAt: ovSet.Int64, Expires: ovExp.Int64}
	}
	var inMaint int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM maintenances m JOIN maintenance_components mc ON mc.maintenance_id = m.id
		WHERE mc.component=? AND m.cancelled=0 AND m.starts <= ? AND ? < m.ends`, id, ts, ts).Scan(&inMaint); err != nil {
		return err
	}
	st, src := model.Effective(model.Status(autoStatus), automated == 1, ov, inMaint > 0, ts)
	var hasHistory int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM status_changes WHERE component=?`, id).Scan(&hasHistory); err != nil {
		return err
	}
	if string(st) != effStatus || hasHistory == 0 {
		if _, err := tx.Exec(`INSERT INTO status_changes(component,ts,status,source) VALUES(?,?,?,?)`, id, ts, string(st), string(src)); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE components SET effective_since=? WHERE id=?`, ts, id); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`UPDATE components SET effective_status=?, effective_source=? WHERE id=?`, string(st), string(src), id)
	return err
}

func applyUpsertIncident(tx *sql.Tx, c *Command, d *UpsertIncident) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM incidents WHERE id=?`, d.ID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		if d.FirstUpdate == nil {
			return verr("a new incident needs a first update")
		}
		if _, err := tx.Exec(`INSERT INTO incidents(id,title,impact,state,created,updated,postmortem_url,author) VALUES(?,?,?,?,?,?,?,?)`,
			d.ID, d.Title, d.Impact, d.FirstUpdate.State, c.TS, c.TS, d.PostmortemURL, c.Actor); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(`UPDATE incidents SET title=?, impact=?, postmortem_url=?, updated=? WHERE id=?`,
			d.Title, d.Impact, d.PostmortemURL, c.TS, d.ID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DELETE FROM incident_components WHERE incident_id=?`, d.ID); err != nil {
		return err
	}
	for _, comp := range d.Components {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO incident_components(incident_id, component) VALUES(?,?)`, d.ID, comp); err != nil {
			return err
		}
	}
	if err := audit(tx, c, d.ID, d.Title); err != nil {
		return err
	}
	if exists == 0 {
		u := *d.FirstUpdate
		u.IncidentID = d.ID
		uc := *c
		uc.Type = CmdAddIncidentUpdate
		return applyIncidentUpdate(tx, &uc, &u)
	}
	return nil
}

func applyIncidentUpdate(tx *sql.Tx, c *Command, u *IncidentUpdate) error {
	var exists int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM incidents WHERE id=?`, u.IncidentID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return verr("incident %s not found", u.IncidentID)
	}
	ts := u.TS
	if ts == 0 {
		ts = c.TS
	}
	if _, err := tx.Exec(`INSERT INTO incident_updates(id,incident_id,ts,state,body,author) VALUES(?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET state=excluded.state, body=excluded.body`,
		u.ID, u.IncidentID, ts, u.State, u.Body, c.Actor); err != nil {
		return err
	}
	// The incident's state follows its latest update.
	var latest string
	if err := tx.QueryRow(`SELECT state FROM incident_updates WHERE incident_id=? ORDER BY ts DESC, rowid DESC LIMIT 1`, u.IncidentID).Scan(&latest); err != nil {
		return err
	}
	resolved := int64(0)
	if latest == model.IncidentResolved {
		resolved = ts
		var prev int64
		if err := tx.QueryRow(`SELECT resolved FROM incidents WHERE id=?`, u.IncidentID).Scan(&prev); err != nil {
			return err
		}
		if prev != 0 {
			resolved = prev
		}
	}
	if _, err := tx.Exec(`UPDATE incidents SET state=?, updated=?, resolved=? WHERE id=?`, latest, c.TS, resolved, u.IncidentID); err != nil {
		return err
	}
	return audit(tx, c, u.IncidentID, u.State)
}

func maintenanceComponents(tx *sql.Tx, id string) ([]string, error) {
	rows, err := tx.Query(`SELECT component FROM maintenance_components WHERE maintenance_id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func applyUpsertMaintenance(tx *sql.Tx, c *Command, d *Maintenance) error {
	old, err := maintenanceComponents(tx, d.ID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO maintenances(id,title,body,starts,ends,cancelled,created,updated,author) VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET title=excluded.title, body=excluded.body, starts=excluded.starts, ends=excluded.ends, cancelled=excluded.cancelled, updated=excluded.updated`,
		d.ID, d.Title, d.Body, d.Starts, d.Ends, boolInt(d.Cancelled), c.TS, c.TS, c.Actor); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM maintenance_components WHERE maintenance_id=?`, d.ID); err != nil {
		return err
	}
	touched := map[string]bool{}
	for _, id := range old {
		touched[id] = true
	}
	for _, comp := range d.Components {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO maintenance_components(maintenance_id, component) VALUES(?,?)`, d.ID, comp); err != nil {
			return err
		}
		touched[comp] = true
	}
	for id := range touched {
		if err := ensureComponent(tx, id); err != nil {
			return err
		}
		if err := refreshEffective(tx, id, c.TS); err != nil {
			return err
		}
	}
	return audit(tx, c, d.ID, d.Title)
}

// --- snapshots ---

type fsmSnapshot struct{ path string }

// Snapshot implements raft.FSM: a consistent copy of the database via
// VACUUM INTO. Raft does not call Apply concurrently with Snapshot.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	tmp := filepath.Join(filepath.Dir(f.path), fmt.Sprintf("snapshot-%d.db", time.Now().UnixNano()))
	if _, err := f.db.Exec(`VACUUM INTO ?`, tmp); err != nil {
		return nil, fmt.Errorf("vacuum into: %w", err)
	}
	return &fsmSnapshot{path: tmp}, nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	fh, err := os.Open(s.path)
	if err != nil {
		sink.Cancel()
		return err
	}
	defer fh.Close()
	if _, err := io.Copy(sink, fh); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() { os.Remove(s.path) }

// Restore implements raft.FSM: replace the database with the snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	tmp := f.path + ".restore"
	fh, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(fh, rc); err != nil {
		fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.db.Close(); err != nil {
		return err
	}
	for _, p := range []string{f.path + "-wal", f.path + "-shm"} {
		os.Remove(p)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	db, err := openDB(f.path)
	if err != nil {
		return err
	}
	f.db = db
	var tick sql.NullInt64
	_ = db.QueryRow(`SELECT MAX(last_tick) FROM components`).Scan(&tick)
	f.lastTick = tick.Int64
	return nil
}
