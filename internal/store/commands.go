package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"statusd/internal/model"
)

// Command types replicated through raft. Every command carries its own
// timestamp: the FSM never reads the clock, so all nodes apply identically.
const (
	CmdRecordTick         = "record_tick"
	CmdSetOverride        = "set_override"
	CmdClearOverride      = "clear_override"
	CmdSetSetting         = "set_setting"
	CmdUpsertIncident     = "upsert_incident"
	CmdAddIncidentUpdate  = "add_incident_update"
	CmdDeleteIncident     = "delete_incident"
	CmdUpsertMaintenance  = "upsert_maintenance"
	CmdDeleteMaintenance  = "delete_maintenance"
	CmdUpsertAnnouncement = "upsert_announcement"
	CmdDeleteAnnouncement = "delete_announcement"
	CmdWebhookDelivered   = "webhook_delivered"
)

// Settings keys.
const (
	SettingAutomationPaused = "automation_paused" // "1" freezes automatic statuses
)

// Command is the envelope stored in the raft log.
type Command struct {
	Type  string          `json:"type"`
	TS    int64           `json:"ts"`              // unix seconds, set by the submitter
	Actor string          `json:"actor,omitempty"` // human user (audit), empty for the system
	Data  json.RawMessage `json:"data"`
}

func NewCommand(typ string, ts int64, actor string, data any) (Command, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return Command{}, err
	}
	return Command{Type: typ, TS: ts, Actor: actor, Data: b}, nil
}

// WebhookDelivered records the statuses a webhook now knows about (after a
// successful delivery, or as the silent baseline for new components).
type WebhookDelivered struct {
	Webhook  string                  `json:"webhook"`
	Statuses map[string]model.Status `json:"statuses"` // component ID -> status
}

func (w *WebhookDelivered) Validate() error {
	if w.Webhook == "" {
		return verr("webhook name is required")
	}
	for c, st := range w.Statuses {
		if c == "" || !st.Valid() {
			return verr("webhook %s: bad status %q for %q", w.Webhook, st, c)
		}
	}
	return nil
}

// TickComponent is one component's evaluation in a tick.
type TickComponent struct {
	ID         string          `json:"id"`
	Raw        model.Level     `json:"raw"`
	Auto       model.Level     `json:"auto"`
	AutoStatus model.Status    `json:"auto_status"`
	Automated  bool            `json:"automated"`
	Signals    json.RawMessage `json:"signals,omitempty"`
	FreshAge   *float64        `json:"fresh_age,omitempty"`
	FreshLevel *model.Level    `json:"fresh_level,omitempty"`
}

// RecordTick is committed by the leader after every evaluation.
type RecordTick struct {
	Node       string          `json:"node"`
	Interval   float64         `json:"interval"` // seconds this tick accounts for (uptime)
	Components []TickComponent `json:"components"`
	PruneDays  int             `json:"prune_days"` // drop history older than this many days (0 = keep)
}

type SetOverride struct {
	Component string       `json:"component"`
	Status    model.Status `json:"status"`
	Reason    string       `json:"reason"`
	Expires   int64        `json:"expires"` // 0 = manual until cleared
}

type ClearOverride struct {
	Component string `json:"component"`
}

type SetSetting struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Incident struct {
	ID            string           `json:"id"`
	Title         string           `json:"title"`
	Impact        string           `json:"impact"`
	State         string           `json:"state"`
	Created       int64            `json:"created"`
	Updated       int64            `json:"updated"`
	Resolved      int64            `json:"resolved"`
	PostmortemURL string           `json:"postmortem_url"`
	Author        string           `json:"author"`
	Components    []string         `json:"components"`
	Updates       []IncidentUpdate `json:"updates,omitempty"`
}

type IncidentUpdate struct {
	ID         string `json:"id"`
	IncidentID string `json:"incident_id"`
	TS         int64  `json:"ts"`
	State      string `json:"state"`
	Body       string `json:"body"`
	Author     string `json:"author"`
}

// UpsertIncident creates or edits an incident's metadata. When creating, a
// first update (FirstUpdate) is required.
type UpsertIncident struct {
	ID            string          `json:"id"`
	Title         string          `json:"title"`
	Impact        string          `json:"impact"`
	PostmortemURL string          `json:"postmortem_url"`
	Components    []string        `json:"components"`
	FirstUpdate   *IncidentUpdate `json:"first_update,omitempty"`
}

type AddIncidentUpdate struct {
	IncidentUpdate
}

type DeleteByID struct {
	ID string `json:"id"`
}

type Maintenance struct {
	ID         string   `json:"id"`
	Title      string   `json:"title"`
	Body       string   `json:"body"`
	Starts     int64    `json:"starts"`
	Ends       int64    `json:"ends"`
	Cancelled  bool     `json:"cancelled"`
	Created    int64    `json:"created"`
	Updated    int64    `json:"updated"`
	Author     string   `json:"author"`
	Components []string `json:"components"`
}

// ActiveAt: started and not yet ended (and not cancelled).
func (m *Maintenance) ActiveAt(ts int64) bool {
	return !m.Cancelled && m.Starts <= ts && ts < m.Ends
}

type Announcement struct {
	ID      string `json:"id"`
	Level   string `json:"level"`
	Title   string `json:"title"`
	Body    string `json:"body"`
	Starts  int64  `json:"starts"`
	Ends    int64  `json:"ends"` // 0 = until removed
	Created int64  `json:"created"`
	Updated int64  `json:"updated"`
	Author  string `json:"author"`
}

func (a *Announcement) ActiveAt(ts int64) bool {
	return a.Starts <= ts && (a.Ends == 0 || ts < a.Ends)
}

// ErrValidation marks bad user input (rejected before reaching raft).
var ErrValidation = errors.New("validation error")

func verr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, a...))
}

func (u *UpsertIncident) Validate() error {
	if strings.TrimSpace(u.ID) == "" {
		return verr("missing id")
	}
	if strings.TrimSpace(u.Title) == "" {
		return verr("title is required")
	}
	if !model.ValidImpact(u.Impact) {
		return verr("invalid impact %q", u.Impact)
	}
	if u.FirstUpdate != nil {
		if err := u.FirstUpdate.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (u *IncidentUpdate) Validate() error {
	if strings.TrimSpace(u.ID) == "" {
		return verr("missing update id")
	}
	if !model.ValidIncidentState(u.State) {
		return verr("invalid incident state %q", u.State)
	}
	if strings.TrimSpace(u.Body) == "" {
		return verr("update text is required")
	}
	return nil
}

func (m *Maintenance) Validate() error {
	if strings.TrimSpace(m.ID) == "" {
		return verr("missing id")
	}
	if strings.TrimSpace(m.Title) == "" {
		return verr("title is required")
	}
	if m.Starts <= 0 || m.Ends <= m.Starts {
		return verr("the window must end after it starts")
	}
	return nil
}

func (a *Announcement) Validate() error {
	if strings.TrimSpace(a.ID) == "" {
		return verr("missing id")
	}
	if !model.ValidAnnouncementLevel(a.Level) {
		return verr("invalid level %q", a.Level)
	}
	if strings.TrimSpace(a.Title) == "" {
		return verr("title is required")
	}
	if a.Ends != 0 && a.Ends <= a.Starts {
		return verr("the announcement must end after it starts")
	}
	return nil
}

func (o *SetOverride) Validate() error {
	if o.Component == "" {
		return verr("missing component")
	}
	if !o.Status.Valid() {
		return verr("invalid status %q", o.Status)
	}
	if strings.TrimSpace(o.Reason) == "" {
		return verr("a reason is required")
	}
	return nil
}
