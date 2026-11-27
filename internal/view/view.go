// Package view builds the public status documents from the catalogue and the
// replicated state. Effective statuses are resolved at read time, so an
// override, an expiring override or a starting maintenance shows immediately.
package view

import (
	"sort"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/model"
	"statusd/internal/store"
)

// Reader is the subset of the store the view needs.
type Reader interface {
	Components() (map[string]*store.ComponentState, error)
	Incidents(store.IncidentFilter) ([]*store.Incident, error)
	Maintenances(store.MaintenanceFilter) ([]*store.Maintenance, error)
	Announcements() ([]*store.Announcement, error)
	Daily(sinceDay string) (map[string]map[string]map[string]float64, error)
	Incident(id string) (*store.Incident, error)
	LastTick() int64
}

type Builder struct {
	Cat        *catalog.Catalog
	Store      Reader
	PageName   string
	StaleAfter time.Duration // data older than this is flagged stale
}

type Summary struct {
	Page          Page             `json:"page"`
	GeneratedAt   time.Time        `json:"generated_at"`
	DataAsOf      *time.Time       `json:"data_as_of"`
	Stale         bool             `json:"stale"`
	Overall       Overall          `json:"overall"`
	Announcements []Announcement   `json:"announcements"`
	Groups        []Group          `json:"groups"`
	Incidents     IncidentsBlock   `json:"incidents"`
	Maintenances  MaintenanceBlock `json:"maintenances"`
}

type Page struct {
	Name string `json:"name"`
}

type Overall struct {
	Status      model.Status `json:"status"`
	Description string       `json:"description"`
}

type Announcement struct {
	ID     string     `json:"id"`
	Level  string     `json:"level"`
	Title  string     `json:"title"`
	Body   string     `json:"body"`
	Starts time.Time  `json:"starts"`
	Ends   *time.Time `json:"ends,omitempty"`
}

type Group struct {
	Slug        string       `json:"slug"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Collapsed   bool         `json:"collapsed"`
	Status      model.Status `json:"status"`
	Components  []Component  `json:"components"`
}

type Component struct {
	ID          string       `json:"id"`
	Slug        string       `json:"slug"`
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Status      model.Status `json:"status"`
	StatusLabel string       `json:"status_label"`
	Since       *time.Time   `json:"since,omitempty"`
	Source      model.Source `json:"source"`
	Freshness   *Freshness   `json:"freshness,omitempty"`
}

type Freshness struct {
	UpdatedAt  time.Time `json:"updated_at"`
	AgeSeconds int64     `json:"age_seconds"`
	Stale      bool      `json:"stale"`
}

type IncidentUpdate struct {
	TS    time.Time `json:"ts"`
	State string    `json:"state"`
	Body  string    `json:"body"`
}

type Incident struct {
	ID            string           `json:"id"`
	Title         string           `json:"title"`
	Impact        string           `json:"impact"`
	State         string           `json:"state"`
	Created       time.Time        `json:"created"`
	Updated       time.Time        `json:"updated"`
	Resolved      *time.Time       `json:"resolved,omitempty"`
	PostmortemURL string           `json:"postmortem_url,omitempty"`
	Components    []ComponentRef   `json:"components"`
	Updates       []IncidentUpdate `json:"updates"`
}

type ComponentRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Group string `json:"group"`
}

type IncidentsBlock struct {
	Active   []Incident `json:"active"`
	Recent   []Incident `json:"recent"` // resolved within the last 7 days
	Count30d int        `json:"count_30d"`
}

type Maintenance struct {
	ID         string         `json:"id"`
	Title      string         `json:"title"`
	Body       string         `json:"body"`
	Starts     time.Time      `json:"starts"`
	Ends       time.Time      `json:"ends"`
	State      string         `json:"state"` // scheduled | in_progress | completed | cancelled
	Components []ComponentRef `json:"components"`
}

type MaintenanceBlock struct {
	Active   []Maintenance `json:"active"`
	Upcoming []Maintenance `json:"upcoming"`
	Recent   []Maintenance `json:"recent"` // completed within the last 7 days
	Count30d int           `json:"count_30d"`
}

func unix(ts int64) time.Time { return time.Unix(ts, 0).UTC() }

func unixPtr(ts int64) *time.Time {
	if ts == 0 {
		return nil
	}
	t := unix(ts)
	return &t
}

func (b *Builder) ref(id string) ComponentRef {
	if c := b.Cat.Component(id); c != nil {
		return ComponentRef{ID: id, Name: c.Name, Group: c.Group.Name}
	}
	return ComponentRef{ID: id, Name: id}
}

func (b *Builder) refs(ids []string) []ComponentRef {
	out := make([]ComponentRef, 0, len(ids))
	for _, id := range ids {
		c := b.Cat.Component(id)
		if c != nil && (c.Hidden || c.Group.Hidden) {
			continue
		}
		out = append(out, b.ref(id))
	}
	return out
}

func (b *Builder) incident(i *store.Incident) Incident {
	out := Incident{
		ID: i.ID, Title: i.Title, Impact: i.Impact, State: i.State,
		Created: unix(i.Created), Updated: unix(i.Updated), Resolved: unixPtr(i.Resolved),
		PostmortemURL: i.PostmortemURL, Components: b.refs(i.Components), Updates: []IncidentUpdate{},
	}
	for _, u := range i.Updates {
		out.Updates = append(out.Updates, IncidentUpdate{TS: unix(u.TS), State: u.State, Body: u.Body})
	}
	return out
}

func (b *Builder) maintenance(m *store.Maintenance, now int64) Maintenance {
	state := "scheduled"
	switch {
	case m.Cancelled:
		state = "cancelled"
	case m.ActiveAt(now):
		state = "in_progress"
	case m.Ends <= now:
		state = "completed"
	}
	return Maintenance{ID: m.ID, Title: m.Title, Body: m.Body, Starts: unix(m.Starts), Ends: unix(m.Ends), State: state, Components: b.refs(m.Components)}
}

// ComponentStatus resolves one component's live status. Exported for the
// control panel.
type ComponentStatus struct {
	Status model.Status
	Source model.Source
	Since  int64
	State  *store.ComponentState
}

func (b *Builder) resolve(states map[string]*store.ComponentState, maint []*store.Maintenance, now int64) map[string]ComponentStatus {
	inMaint := map[string]int64{}
	for _, m := range maint {
		if m.ActiveAt(now) {
			for _, c := range m.Components {
				if s, ok := inMaint[c]; !ok || m.Starts < s {
					inMaint[c] = m.Starts
				}
			}
		}
	}
	out := map[string]ComponentStatus{}
	for _, c := range b.Cat.Components() {
		cs := states[c.ID]
		if cs == nil {
			cs = &store.ComponentState{ID: c.ID, AutoStatus: model.Operational, AutoLevel: model.OK, RawLevel: model.NoData}
		}
		ms, maintActive := inMaint[c.ID]
		st, src := model.Effective(cs.AutoStatus, c.Automated(), cs.Override, maintActive, now)
		since := cs.AutoSince
		switch src {
		case model.SourceManual:
			since = cs.Override.SetAt
		case model.SourceMaintenance:
			since = ms
		}
		out[c.ID] = ComponentStatus{Status: st, Source: src, Since: since, State: cs}
	}
	return out
}

// Resolve returns the live status of every catalogue component.
func (b *Builder) Resolve(now time.Time) (map[string]ComponentStatus, error) {
	states, err := b.Store.Components()
	if err != nil {
		return nil, err
	}
	maint, err := b.Store.Maintenances(store.MaintenanceFilter{EndsAfter: now.Unix()})
	if err != nil {
		return nil, err
	}
	return b.resolve(states, maint, now.Unix()), nil
}

func (b *Builder) Summary(now time.Time) (*Summary, error) {
	ts := now.Unix()
	states, err := b.Store.Components()
	if err != nil {
		return nil, err
	}
	maint, err := b.Store.Maintenances(store.MaintenanceFilter{EndsAfter: ts - 30*86400})
	if err != nil {
		return nil, err
	}
	resolved := b.resolve(states, maint, ts)

	s := &Summary{
		Page:          Page{Name: b.PageName},
		GeneratedAt:   now.UTC(),
		Announcements: []Announcement{},
		Groups:        []Group{},
		Incidents:     IncidentsBlock{Active: []Incident{}, Recent: []Incident{}},
		Maintenances:  MaintenanceBlock{Active: []Maintenance{}, Upcoming: []Maintenance{}, Recent: []Maintenance{}},
	}
	if lt := b.Store.LastTick(); lt > 0 {
		t := unix(lt)
		s.DataAsOf = &t
		s.Stale = b.StaleAfter > 0 && now.Sub(t) > b.StaleAfter
	} else {
		s.Stale = true
	}

	var all []model.Status
	for _, g := range b.Cat.Groups {
		if g.Hidden {
			continue
		}
		vg := Group{Slug: g.Slug, Name: g.Name, Description: g.Description, Collapsed: g.Collapsed, Components: []Component{}}
		var gs []model.Status
		for _, c := range g.Components {
			if c.Hidden {
				continue
			}
			r := resolved[c.ID]
			vc := Component{
				ID: c.ID, Slug: c.Slug, Name: c.Name, Description: c.Description,
				Status: r.Status, StatusLabel: r.Status.Label(), Since: unixPtr(r.Since), Source: r.Source,
			}
			if c.Freshness != nil && c.Freshness.IsPublic() && r.State.FreshTS != nil {
				age := ts - *r.State.FreshTS
				if age < 0 {
					age = 0
				}
				vc.Freshness = &Freshness{
					UpdatedAt:  unix(*r.State.FreshTS),
					AgeSeconds: age,
					Stale:      float64(age) > c.Freshness.DegradedAfter.Seconds(),
				}
			}
			gs = append(gs, r.Status)
			vg.Components = append(vg.Components, vc)
		}
		vg.Status = model.Worst(gs...)
		all = append(all, gs...)
		s.Groups = append(s.Groups, vg)
	}
	s.Overall.Status = model.Worst(all...)
	s.Overall.Description = overallText(s.Overall.Status)

	anns, err := b.Store.Announcements()
	if err != nil {
		return nil, err
	}
	for _, a := range anns {
		if a.ActiveAt(ts) {
			s.Announcements = append(s.Announcements, Announcement{ID: a.ID, Level: a.Level, Title: a.Title, Body: a.Body, Starts: unix(a.Starts), Ends: unixPtr(a.Ends)})
		}
	}
	sort.SliceStable(s.Announcements, func(i, j int) bool { return levelRank(s.Announcements[i].Level) > levelRank(s.Announcements[j].Level) })

	incs, err := b.Store.Incidents(store.IncidentFilter{ActiveOrSince: ts - 30*86400})
	if err != nil {
		return nil, err
	}
	for _, i := range incs {
		if i.State != model.IncidentResolved {
			s.Incidents.Active = append(s.Incidents.Active, b.incident(i))
		} else if i.Resolved >= ts-7*86400 {
			s.Incidents.Recent = append(s.Incidents.Recent, b.incident(i))
		}
	}
	if s.Incidents.Count30d, err = b.countIncidents(ts - 30*86400); err != nil {
		return nil, err
	}

	for _, m := range maint {
		vm := b.maintenance(m, ts)
		switch vm.State {
		case "in_progress":
			s.Maintenances.Active = append(s.Maintenances.Active, vm)
		case "scheduled":
			s.Maintenances.Upcoming = append(s.Maintenances.Upcoming, vm)
		case "completed":
			s.Maintenances.Count30d++
			if m.Ends >= ts-7*86400 {
				s.Maintenances.Recent = append(s.Maintenances.Recent, vm)
			}
		}
	}
	sort.Slice(s.Maintenances.Upcoming, func(i, j int) bool {
		return s.Maintenances.Upcoming[i].Starts.Before(s.Maintenances.Upcoming[j].Starts)
	})
	return s, nil
}

func (b *Builder) countIncidents(since int64) (int, error) {
	incs, err := b.Store.Incidents(store.IncidentFilter{CreatedSince: since})
	return len(incs), err
}

func levelRank(l string) int {
	switch l {
	case "critical":
		return 2
	case "warning":
		return 1
	}
	return 0
}

func overallText(s model.Status) string {
	switch s {
	case model.MajorOutage:
		return "Major Service Outage"
	case model.PartialOutage:
		return "Partial Service Outage"
	case model.DegradedPerformance:
		return "Degraded Service Performance"
	case model.UnderMaintenance:
		return "Service Under Maintenance"
	}
	return "All Systems Operational"
}

// IncidentByID returns one public incident.
func (b *Builder) IncidentByID(id string) (*Incident, error) {
	i, err := b.Store.Incident(id)
	if err != nil {
		return nil, err
	}
	v := b.incident(i)
	return &v, nil
}

// IncidentHistory returns incidents created in [from, to).
func (b *Builder) IncidentHistory(from, to time.Time) ([]Incident, error) {
	incs, err := b.Store.Incidents(store.IncidentFilter{CreatedSince: from.Unix(), CreatedBefore: to.Unix()})
	if err != nil {
		return nil, err
	}
	out := []Incident{}
	for _, i := range incs {
		out = append(out, b.incident(i))
	}
	return out, nil
}

// MaintenanceHistory returns maintenances overlapping [from, to).
func (b *Builder) MaintenanceHistory(from, to time.Time, now time.Time) ([]Maintenance, error) {
	ms, err := b.Store.Maintenances(store.MaintenanceFilter{EndsAfter: from.Unix(), StartsBefore: to.Unix()})
	if err != nil {
		return nil, err
	}
	out := []Maintenance{}
	for _, m := range ms {
		out = append(out, b.maintenance(m, now.Unix()))
	}
	return out, nil
}

// --- uptime ---

type UptimeDay struct {
	Date      string       `json:"date"`
	UptimePct *float64     `json:"uptime_pct"` // nil when nothing was recorded that day
	Worst     model.Status `json:"worst,omitempty"`
	Coverage  float64      `json:"coverage"` // share of the day with recorded state
}

type UptimeComponent struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	UptimePct *float64    `json:"uptime_pct"`
	Days      []UptimeDay `json:"days"`
}

type UptimeGroup struct {
	Slug       string            `json:"slug"`
	Name       string            `json:"name"`
	Components []UptimeComponent `json:"components"`
}

type Uptime struct {
	GeneratedAt time.Time     `json:"generated_at"`
	Days        int           `json:"days"`
	Groups      []UptimeGroup `json:"groups"`
}

// Downtime weights (Atlassian Statuspage convention): a major outage counts
// fully, a partial outage 30 %, degraded performance not at all. Maintenance
// is excluded from the denominator.
var downWeight = map[string]float64{string(model.MajorOutage): 1, string(model.PartialOutage): 0.3}

func (b *Builder) Uptime(now time.Time, days int) (*Uptime, error) {
	start := now.UTC().AddDate(0, 0, -(days - 1))
	daily, err := b.Store.Daily(start.Format("2006-01-02"))
	if err != nil {
		return nil, err
	}
	out := &Uptime{GeneratedAt: now.UTC(), Days: days, Groups: []UptimeGroup{}}
	for _, g := range b.Cat.Groups {
		if g.Hidden {
			continue
		}
		ug := UptimeGroup{Slug: g.Slug, Name: g.Name, Components: []UptimeComponent{}}
		for _, c := range g.Components {
			if c.Hidden {
				continue
			}
			uc := UptimeComponent{ID: c.ID, Name: c.Name, Days: make([]UptimeDay, 0, days)}
			var totDown, totCounted float64
			for i := 0; i < days; i++ {
				d := start.AddDate(0, 0, i).Format("2006-01-02")
				ud := UptimeDay{Date: d}
				secs := daily[c.ID][d]
				var counted, down, recorded float64
				worst := model.Operational
				for st, v := range secs {
					recorded += v
					if st != string(model.UnderMaintenance) {
						counted += v
					}
					down += v * downWeight[st]
					if model.Status(st).Severity() > worst.Severity() && v > 0 {
						worst = model.Status(st)
					}
				}
				if recorded > 0 {
					ud.Worst = worst
					ud.Coverage = min(recorded/86400, 1)
					if counted > 0 {
						p := 100 * (1 - down/counted)
						ud.UptimePct = &p
					}
				}
				totDown += down
				totCounted += counted
				uc.Days = append(uc.Days, ud)
			}
			if totCounted > 0 {
				p := 100 * (1 - totDown/totCounted)
				uc.UptimePct = &p
			}
			ug.Components = append(ug.Components, uc)
		}
		out.Groups = append(out.Groups, ug)
	}
	return out, nil
}
