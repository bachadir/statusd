package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"statusd/internal/model"
)

// ComponentState is the replicated runtime state of one component.
type ComponentState struct {
	ID             string
	RawLevel       model.Level
	AutoLevel      model.Level
	AutoStatus     model.Status
	AutoSince      int64
	Automated      bool
	Signals        json.RawMessage
	FreshAge       *float64
	FreshLevel     *model.Level
	FreshTS        *int64
	LastTick       int64
	Override       *model.Override
	EffectiveSince int64
}

func (s *Store) Components() (map[string]*ComponentState, error) {
	out := map[string]*ComponentState{}
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT id, raw_level, auto_level, auto_status, auto_since, automated, signals_json, fresh_age, fresh_level, fresh_ts, last_tick,
			override_status, override_reason, override_by, override_set, override_expires, effective_since FROM components`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				c                        ComponentState
				automated                int
				signals                  sql.NullString
				freshAge                 sql.NullFloat64
				freshLevel, freshTS      sql.NullInt64
				ovStatus, ovReason, ovBy sql.NullString
				ovSet, ovExp             sql.NullInt64
				auto                     string
			)
			if err := rows.Scan(&c.ID, &c.RawLevel, &c.AutoLevel, &auto, &c.AutoSince, &automated, &signals, &freshAge, &freshLevel, &freshTS, &c.LastTick,
				&ovStatus, &ovReason, &ovBy, &ovSet, &ovExp, &c.EffectiveSince); err != nil {
				return err
			}
			c.AutoStatus = model.Status(auto)
			c.Automated = automated == 1
			if signals.Valid {
				c.Signals = json.RawMessage(signals.String)
			}
			if freshAge.Valid {
				v := freshAge.Float64
				c.FreshAge = &v
			}
			if freshLevel.Valid {
				l := model.Level(freshLevel.Int64)
				c.FreshLevel = &l
			}
			if freshTS.Valid {
				v := freshTS.Int64
				c.FreshTS = &v
			}
			if ovStatus.Valid {
				c.Override = &model.Override{Status: model.Status(ovStatus.String), Reason: ovReason.String, By: ovBy.String, SetAt: ovSet.Int64, Expires: ovExp.Int64}
			}
			out[c.ID] = &c
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) Settings() (map[string]string, error) {
	out := map[string]string{}
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT key, value FROM settings`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var k, v string
			if err := rows.Scan(&k, &v); err != nil {
				return err
			}
			out[k] = v
		}
		return rows.Err()
	})
	return out, err
}

type IncidentFilter struct {
	OpenOnly      bool
	CreatedSince  int64 // inclusive, 0 = no bound
	CreatedBefore int64 // exclusive, 0 = no bound
	ActiveOrSince int64 // open incidents, or resolved at/after this ts
	Limit         int
}

func (s *Store) Incidents(f IncidentFilter) ([]*Incident, error) {
	var out []*Incident
	err := s.fsm.withDB(func(db *sql.DB) error {
		q := `SELECT id,title,impact,state,created,updated,resolved,postmortem_url,author FROM incidents`
		var conds []string
		var args []any
		if f.OpenOnly {
			conds = append(conds, `state != 'resolved'`)
		}
		if f.CreatedSince > 0 {
			conds = append(conds, `created >= ?`)
			args = append(args, f.CreatedSince)
		}
		if f.CreatedBefore > 0 {
			conds = append(conds, `created < ?`)
			args = append(args, f.CreatedBefore)
		}
		if f.ActiveOrSince > 0 {
			conds = append(conds, `(state != 'resolved' OR resolved >= ?)`)
			args = append(args, f.ActiveOrSince)
		}
		if len(conds) > 0 {
			q += " WHERE " + strings.Join(conds, " AND ")
		}
		q += ` ORDER BY created DESC`
		if f.Limit > 0 {
			q += ` LIMIT ?`
			args = append(args, f.Limit)
		}
		rows, err := db.Query(q, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var i Incident
			if err := rows.Scan(&i.ID, &i.Title, &i.Impact, &i.State, &i.Created, &i.Updated, &i.Resolved, &i.PostmortemURL, &i.Author); err != nil {
				rows.Close()
				return err
			}
			out = append(out, &i)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, i := range out {
			if err := loadIncidentDetail(db, i); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

func loadIncidentDetail(db *sql.DB, i *Incident) error {
	rows, err := db.Query(`SELECT id, ts, state, body, author FROM incident_updates WHERE incident_id=? ORDER BY ts DESC, rowid DESC`, i.ID)
	if err != nil {
		return err
	}
	for rows.Next() {
		u := IncidentUpdate{IncidentID: i.ID}
		if err := rows.Scan(&u.ID, &u.TS, &u.State, &u.Body, &u.Author); err != nil {
			rows.Close()
			return err
		}
		i.Updates = append(i.Updates, u)
	}
	rows.Close()
	comps, err := db.Query(`SELECT component FROM incident_components WHERE incident_id=? ORDER BY component`, i.ID)
	if err != nil {
		return err
	}
	defer comps.Close()
	for comps.Next() {
		var c string
		if err := comps.Scan(&c); err != nil {
			return err
		}
		i.Components = append(i.Components, c)
	}
	return comps.Err()
}

var ErrNotFound = errors.New("not found")

func (s *Store) Incident(id string) (*Incident, error) {
	var out *Incident
	err := s.fsm.withDB(func(db *sql.DB) error {
		var i Incident
		err := db.QueryRow(`SELECT id,title,impact,state,created,updated,resolved,postmortem_url,author FROM incidents WHERE id=?`, id).
			Scan(&i.ID, &i.Title, &i.Impact, &i.State, &i.Created, &i.Updated, &i.Resolved, &i.PostmortemURL, &i.Author)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		out = &i
		return loadIncidentDetail(db, &i)
	})
	return out, err
}

type MaintenanceFilter struct {
	EndsAfter        int64 // only windows ending after this ts (0 = all)
	StartsBefore     int64 // only windows starting before this ts (0 = all)
	IncludeCancelled bool
	Limit            int
}

func (s *Store) Maintenances(f MaintenanceFilter) ([]*Maintenance, error) {
	var out []*Maintenance
	err := s.fsm.withDB(func(db *sql.DB) error {
		q := `SELECT id,title,body,starts,ends,cancelled,created,updated,author FROM maintenances`
		var conds []string
		var args []any
		if f.EndsAfter > 0 {
			conds = append(conds, `ends > ?`)
			args = append(args, f.EndsAfter)
		}
		if f.StartsBefore > 0 {
			conds = append(conds, `starts < ?`)
			args = append(args, f.StartsBefore)
		}
		if !f.IncludeCancelled {
			conds = append(conds, `cancelled = 0`)
		}
		if len(conds) > 0 {
			q += " WHERE " + strings.Join(conds, " AND ")
		}
		q += ` ORDER BY starts DESC`
		if f.Limit > 0 {
			q += ` LIMIT ?`
			args = append(args, f.Limit)
		}
		rows, err := db.Query(q, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var m Maintenance
			var cancelled int
			if err := rows.Scan(&m.ID, &m.Title, &m.Body, &m.Starts, &m.Ends, &cancelled, &m.Created, &m.Updated, &m.Author); err != nil {
				rows.Close()
				return err
			}
			m.Cancelled = cancelled == 1
			out = append(out, &m)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, m := range out {
			comps, err := db.Query(`SELECT component FROM maintenance_components WHERE maintenance_id=? ORDER BY component`, m.ID)
			if err != nil {
				return err
			}
			for comps.Next() {
				var c string
				if err := comps.Scan(&c); err != nil {
					comps.Close()
					return err
				}
				m.Components = append(m.Components, c)
			}
			comps.Close()
		}
		return nil
	})
	return out, err
}

func (s *Store) Maintenance(id string) (*Maintenance, error) {
	all, err := s.Maintenances(MaintenanceFilter{IncludeCancelled: true})
	if err != nil {
		return nil, err
	}
	for _, m := range all {
		if m.ID == id {
			return m, nil
		}
	}
	return nil, ErrNotFound
}

func (s *Store) Announcements() ([]*Announcement, error) {
	var out []*Announcement
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT id,level,title,body,starts,ends,created,updated,author FROM announcements ORDER BY starts DESC`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a Announcement
			if err := rows.Scan(&a.ID, &a.Level, &a.Title, &a.Body, &a.Starts, &a.Ends, &a.Created, &a.Updated, &a.Author); err != nil {
				return err
			}
			out = append(out, &a)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) Announcement(id string) (*Announcement, error) {
	all, err := s.Announcements()
	if err != nil {
		return nil, err
	}
	for _, a := range all {
		if a.ID == id {
			return a, nil
		}
	}
	return nil, ErrNotFound
}

type AuditEntry struct {
	ID     int64  `json:"id"`
	TS     int64  `json:"ts"`
	Actor  string `json:"actor"`
	Action string `json:"action"`
	Target string `json:"target"`
	Detail string `json:"detail"`
}

func (s *Store) Audit(limit int) ([]AuditEntry, error) {
	var out []AuditEntry
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT id,ts,actor,action,target,detail FROM audit ORDER BY id DESC LIMIT ?`, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a AuditEntry
			if err := rows.Scan(&a.ID, &a.TS, &a.Actor, &a.Action, &a.Target, &a.Detail); err != nil {
				return err
			}
			out = append(out, a)
		}
		return rows.Err()
	})
	return out, err
}

// Daily returns component -> day -> status -> seconds for days >= sinceDay.
func (s *Store) Daily(sinceDay string) (map[string]map[string]map[string]float64, error) {
	out := map[string]map[string]map[string]float64{}
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT component, day, status, seconds FROM daily WHERE day >= ?`, sinceDay)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c, d, st string
			var sec float64
			if err := rows.Scan(&c, &d, &st, &sec); err != nil {
				return err
			}
			if out[c] == nil {
				out[c] = map[string]map[string]float64{}
			}
			if out[c][d] == nil {
				out[c][d] = map[string]float64{}
			}
			out[c][d][st] += sec
		}
		return rows.Err()
	})
	return out, err
}

type StatusChange struct {
	Component string `json:"component"`
	TS        int64  `json:"ts"`
	Status    string `json:"status"`
	Source    string `json:"source"`
}

func (s *Store) StatusChanges(component string, since int64, limit int) ([]StatusChange, error) {
	var out []StatusChange
	err := s.fsm.withDB(func(db *sql.DB) error {
		q := `SELECT component, ts, status, source FROM status_changes WHERE ts >= ?`
		args := []any{since}
		if component != "" {
			q += ` AND component = ?`
			args = append(args, component)
		}
		q += ` ORDER BY ts DESC, id DESC LIMIT ?`
		args = append(args, limit)
		rows, err := db.Query(q, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c StatusChange
			if err := rows.Scan(&c.Component, &c.TS, &c.Status, &c.Source); err != nil {
				return err
			}
			out = append(out, c)
		}
		return rows.Err()
	})
	return out, err
}

// WebhookState returns the last status the named webhook was told about, per component.
func (s *Store) WebhookState(webhook string) (map[string]model.Status, error) {
	out := map[string]model.Status{}
	err := s.fsm.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT component, status FROM webhook_state WHERE webhook=?`, webhook)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var c, st string
			if err := rows.Scan(&c, &st); err != nil {
				return err
			}
			out[c] = model.Status(st)
		}
		return rows.Err()
	})
	return out, err
}
