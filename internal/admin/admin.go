// Package admin is the internal control panel: component overrides,
// incidents, maintenances, announcements, settings and the audit log.
package admin

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/eval"
	"statusd/internal/model"
	"statusd/internal/node"
	"statusd/internal/statusio"
	"statusd/internal/store"
	"statusd/internal/view"
	"statusd/internal/webhook"
)

//go:embed templates/*.html static/*
var assets embed.FS

type Deps struct {
	Node    *node.Node
	Store   *store.Store
	Cat     *catalog.Catalog
	View    *view.Builder
	Log     *slog.Logger
	Version string
	// StatusIO is the Status.io publisher; nil when statusio.enabled is false.
	StatusIO interface{ Status() statusio.State }
	// Webhooks is the webhook dispatcher; nil when no webhooks are configured.
	Webhooks interface{ Status() webhook.State }
}

type Admin struct {
	Deps
	auth *auth
	tpl  map[string]*template.Template
	now  func() time.Time
}

func New(ctx context.Context, cfg Config, d Deps) (*Admin, error) {
	a := &Admin{Deps: d, now: time.Now}
	var err error
	if a.auth, err = newAuth(ctx, cfg); err != nil {
		return nil, err
	}
	if cfg.DevInsecureUser != "" {
		d.Log.Warn("ADMIN AUTHENTICATION DISABLED (dev_insecure_user); never use this in production", "user", cfg.DevInsecureUser)
	}
	if a.tpl, err = parseTemplates(); err != nil {
		return nil, err
	}
	return a, nil
}

func parseTemplates() (map[string]*template.Template, error) {
	funcs := template.FuncMap{
		"ts":        fmtTS,
		"tsInput":   tsInput,
		"ago":       ago,
		"dur":       fmtDur,
		"label":     func(s model.Status) string { return s.Label() },
		"statuses":  func() []model.Status { return model.AllStatuses },
		"incStates": func() []string { return model.IncidentStates },
		"impacts":   func() []string { return model.IncidentImpacts },
		"annLevels": func() []string { return model.AnnouncementLevels },
		"level":     func(l model.Level) string { return l.String() },
		"has": func(list []string, s string) bool {
			for _, x := range list {
				if x == s {
					return true
				}
			}
			return false
		},
		"canEdit": func(s *Session) bool { return s != nil && s.Role >= RoleEditor },
		"isAdmin": func(s *Session) bool { return s != nil && s.Role >= RoleAdmin },
		"pathEsc": url.PathEscape,
		"deref": func(f *float64) float64 {
			if f == nil {
				return 0
			}
			return *f
		},
	}
	pages := []string{"dashboard", "component", "incidents", "incident", "maintenances", "maintenance", "announcements", "announcement", "audit", "settings", "error"}
	out := map[string]*template.Template{}
	for _, p := range pages {
		t, err := template.New("layout.html").Funcs(funcs).ParseFS(assets, "templates/layout.html", "templates/"+p+".html")
		if err != nil {
			return nil, fmt.Errorf("template %s: %w", p, err)
		}
		out[p] = t
	}
	return out, nil
}

func fmtTS(v any) string {
	var t time.Time
	switch x := v.(type) {
	case int64:
		if x == 0 {
			return "—"
		}
		t = time.Unix(x, 0)
	case time.Time:
		if x.IsZero() {
			return "—"
		}
		t = x
	case *time.Time:
		if x == nil {
			return "—"
		}
		t = *x
	default:
		return fmt.Sprint(v)
	}
	return t.UTC().Format("2006-01-02 15:04 UTC")
}

func tsInput(ts int64) string {
	if ts == 0 {
		return ""
	}
	return time.Unix(ts, 0).UTC().Format("2006-01-02T15:04")
}

func ago(ts int64) string {
	if ts == 0 {
		return "never"
	}
	return fmtDur(time.Since(time.Unix(ts, 0))) + " ago"
}

func fmtDur(v any) string {
	var d time.Duration
	switch x := v.(type) {
	case time.Duration:
		d = x
	case float64:
		d = time.Duration(x * float64(time.Second))
	case int64:
		d = time.Duration(x) * time.Second
	}
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

func (a *Admin) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /static/", http.FileServerFS(assets))
	mux.HandleFunc("GET /auth/login", a.auth.login)
	mux.HandleFunc("GET /auth/callback", a.auth.callback)
	mux.HandleFunc("POST /auth/logout", a.auth.logout)

	view := func(h func(http.ResponseWriter, *http.Request, *Session)) http.Handler {
		return a.require(RoleViewer, h)
	}
	edit := func(h func(http.ResponseWriter, *http.Request, *Session)) http.Handler {
		return a.require(RoleEditor, h)
	}
	adm := func(h func(http.ResponseWriter, *http.Request, *Session)) http.Handler {
		return a.require(RoleAdmin, h)
	}

	mux.Handle("GET /{$}", view(a.dashboard))
	mux.Handle("GET /components/{group}/{slug}", view(a.component))
	mux.Handle("POST /components/{group}/{slug}/override", edit(a.setOverride))
	mux.Handle("POST /components/{group}/{slug}/clear", edit(a.clearOverride))

	mux.Handle("GET /incidents", view(a.incidents))
	mux.Handle("GET /incidents/new", edit(a.incidentForm))
	mux.Handle("POST /incidents", edit(a.saveIncident))
	mux.Handle("GET /incidents/{id}", view(a.incident))
	mux.Handle("POST /incidents/{id}", edit(a.saveIncident))
	mux.Handle("POST /incidents/{id}/updates", edit(a.addIncidentUpdate))
	mux.Handle("POST /incidents/{id}/delete", adm(a.deleteIncident))

	mux.Handle("GET /maintenances", view(a.maintenances))
	mux.Handle("GET /maintenances/new", edit(a.maintenanceForm))
	mux.Handle("GET /maintenances/{id}", view(a.maintenanceForm))
	mux.Handle("POST /maintenances", edit(a.saveMaintenance))
	mux.Handle("POST /maintenances/{id}", edit(a.saveMaintenance))
	mux.Handle("POST /maintenances/{id}/delete", adm(a.deleteMaintenance))

	mux.Handle("GET /announcements", view(a.announcements))
	mux.Handle("GET /announcements/new", edit(a.announcementForm))
	mux.Handle("GET /announcements/{id}", view(a.announcementForm))
	mux.Handle("POST /announcements", edit(a.saveAnnouncement))
	mux.Handle("POST /announcements/{id}", edit(a.saveAnnouncement))
	mux.Handle("POST /announcements/{id}/delete", edit(a.deleteAnnouncement))

	mux.Handle("GET /audit", view(a.audit))
	mux.Handle("GET /settings", adm(a.settings))
	mux.Handle("POST /settings", adm(a.saveSettings))
	mux.Handle("GET /api/state", view(a.stateJSON))
	return securityHeaders(mux)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; form-action 'self' https:; frame-ancestors 'none'")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func (a *Admin) require(min Role, h func(http.ResponseWriter, *http.Request, *Session)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := a.auth.session(r)
		if s == nil {
			if r.Method == http.MethodGet {
				http.Redirect(w, r, "/auth/login?return="+url.QueryEscape(r.URL.RequestURI()), http.StatusFound)
				return
			}
			http.Error(w, "session expired, please log in again", http.StatusUnauthorized)
			return
		}
		if s.Role < min {
			a.render(w, r, s, http.StatusForbidden, "error", map[string]any{"Error": "You need the " + min.String() + " role for this."})
			return
		}
		if r.Method == http.MethodPost {
			if err := r.ParseForm(); err != nil {
				http.Error(w, "bad form", http.StatusBadRequest)
				return
			}
			if r.PostFormValue("csrf") != s.CSRF {
				http.Error(w, "invalid CSRF token; reload the page and retry", http.StatusForbidden)
				return
			}
		}
		h(w, r, s)
	})
}

type page struct {
	Title   string
	Nav     string
	User    *Session
	Cluster clusterInfo
	Flash   string
	Error   string
	Data    map[string]any
}

type clusterInfo struct {
	Node      string
	Leader    string
	RaftState string
	LastTick  int64
	Stale     bool
	Paused    bool
	Version   string
}

func (a *Admin) cluster() clusterInfo {
	st := a.Store.State()
	settings, _ := a.Store.Settings()
	return clusterInfo{
		Node: st.NodeID, Leader: st.Leader, RaftState: st.RaftState, LastTick: st.LastTick,
		Stale:   st.LastTick == 0 || time.Since(time.Unix(st.LastTick, 0)) > 3*a.Node.Interval(),
		Paused:  settings[store.SettingAutomationPaused] == "1",
		Version: a.Version,
	}
}

func (a *Admin) render(w http.ResponseWriter, r *http.Request, s *Session, code int, name string, data map[string]any) {
	p := page{User: s, Cluster: a.cluster(), Data: data, Nav: name}
	if t, ok := data["Title"].(string); ok {
		p.Title = t
	}
	if e, ok := data["Error"].(string); ok {
		p.Error = e
	}
	if c, err := r.Cookie("statusd_flash"); err == nil {
		p.Flash, _ = url.QueryUnescape(c.Value)
		http.SetCookie(w, &http.Cookie{Name: "statusd_flash", Path: "/", MaxAge: -1})
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	if err := a.tpl[name].Execute(w, p); err != nil {
		a.Log.Error("render", "page", name, "err", err)
	}
}

func (a *Admin) redirect(w http.ResponseWriter, r *http.Request, to, flash string) {
	if flash != "" {
		http.SetCookie(w, &http.Cookie{Name: "statusd_flash", Value: url.QueryEscape(flash), Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: 60})
	}
	http.Redirect(w, r, to, http.StatusSeeOther)
}

func (a *Admin) submit(r *http.Request, s *Session, typ string, data any) error {
	cmd, err := store.NewCommand(typ, a.now().Unix(), s.Actor(), data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	return a.Node.Submit(ctx, cmd)
}

func (a *Admin) fail(w http.ResponseWriter, r *http.Request, s *Session, err error) {
	code := http.StatusInternalServerError
	msg := err.Error()
	switch {
	case errors.Is(err, store.ErrValidation):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, node.ErrNoLeader):
		code = http.StatusServiceUnavailable
		msg = "The cluster has no leader right now (election in progress or quorum lost). Nothing was saved; retry in a few seconds."
	case errors.Is(err, store.ErrNotFound):
		code = http.StatusNotFound
	}
	if code == http.StatusInternalServerError {
		a.Log.Error("admin request failed", "path", r.URL.Path, "err", err)
	}
	a.render(w, r, s, code, "error", map[string]any{"Title": "Error", "Error": msg})
}

// --- components ---

type compRow struct {
	C         *catalog.Component
	Status    model.Status
	Source    model.Source
	Since     int64
	State     *store.ComponentState
	Signals   []eval.SignalResult
	Incidents int
}

type groupRows struct {
	G     *catalog.Group
	Rows  []compRow
	Worst model.Status
}

func (a *Admin) rows() ([]groupRows, map[string]compRow, error) {
	res, err := a.View.Resolve(a.now())
	if err != nil {
		return nil, nil, err
	}
	var out []groupRows
	byID := map[string]compRow{}
	for _, g := range a.Cat.Groups {
		gr := groupRows{G: g}
		var ss []model.Status
		for _, c := range g.Components {
			r := res[c.ID]
			row := compRow{C: c, Status: r.Status, Source: r.Source, Since: r.Since, State: r.State}
			if len(r.State.Signals) > 0 {
				_ = json.Unmarshal(r.State.Signals, &row.Signals)
			}
			gr.Rows = append(gr.Rows, row)
			byID[c.ID] = row
			ss = append(ss, r.Status)
		}
		gr.Worst = model.Worst(ss...)
		out = append(out, gr)
	}
	return out, byID, nil
}

func (a *Admin) dashboard(w http.ResponseWriter, r *http.Request, s *Session) {
	groups, _, err := a.rows()
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	open, err := a.Store.Incidents(store.IncidentFilter{OpenOnly: true})
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	now := a.now().Unix()
	maint, err := a.Store.Maintenances(store.MaintenanceFilter{EndsAfter: now})
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	health := a.Node.Health()
	sort.Slice(health.Sources, func(i, j int) bool { return health.Sources[i].Source < health.Sources[j].Source })
	var sio *statusio.State
	if a.StatusIO != nil {
		st := a.StatusIO.Status()
		if len(st.Recent) > 10 {
			st.Recent = st.Recent[:10]
		}
		sio = &st
	}
	var wh *webhook.State
	if a.Webhooks != nil {
		st := a.Webhooks.Status()
		if len(st.Recent) > 10 {
			st.Recent = st.Recent[:10]
		}
		wh = &st
	}
	a.render(w, r, s, http.StatusOK, "dashboard", map[string]any{
		"Title": "Overview", "Groups": groups, "Open": open, "Maint": maint, "Now": now,
		"Health": health, "State": a.Store.State(), "Eval": a.Node.EvalStatus(), "Peers": a.Store.Peers(),
		"StatusIO": sio, "Webhooks": wh,
	})
}

func (a *Admin) compFromPath(r *http.Request) *catalog.Component {
	return a.Cat.Component(r.PathValue("group") + "/" + r.PathValue("slug"))
}

func (a *Admin) component(w http.ResponseWriter, r *http.Request, s *Session) {
	c := a.compFromPath(r)
	if c == nil {
		a.fail(w, r, s, store.ErrNotFound)
		return
	}
	_, byID, err := a.rows()
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	changes, err := a.Store.StatusChanges(c.ID, a.now().Unix()-90*86400, 100)
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "component", map[string]any{
		"Title": c.Group.Name + " — " + c.Name, "Row": byID[c.ID], "Changes": changes, "Now": a.now().Unix(),
	})
}

var expiryChoices = map[string]time.Duration{"15m": 15 * time.Minute, "1h": time.Hour, "4h": 4 * time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour}

func (a *Admin) setOverride(w http.ResponseWriter, r *http.Request, s *Session) {
	c := a.compFromPath(r)
	if c == nil {
		a.fail(w, r, s, store.ErrNotFound)
		return
	}
	d := store.SetOverride{Component: c.ID, Status: model.Status(r.PostFormValue("status")), Reason: strings.TrimSpace(r.PostFormValue("reason"))}
	if e := r.PostFormValue("expires"); e != "" && e != "never" {
		dur, ok := expiryChoices[e]
		if !ok {
			a.fail(w, r, s, fmt.Errorf("%w: invalid expiry", store.ErrValidation))
			return
		}
		d.Expires = a.now().Add(dur).Unix()
	}
	if err := d.Validate(); err != nil {
		a.fail(w, r, s, err)
		return
	}
	if err := a.submit(r, s, store.CmdSetOverride, d); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, back(r, "/components/"+c.ID), c.Name+" set to "+d.Status.Label()+" (manual)")
}

func (a *Admin) clearOverride(w http.ResponseWriter, r *http.Request, s *Session) {
	c := a.compFromPath(r)
	if c == nil {
		a.fail(w, r, s, store.ErrNotFound)
		return
	}
	if err := a.submit(r, s, store.CmdClearOverride, store.ClearOverride{Component: c.ID}); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, back(r, "/components/"+c.ID), c.Name+" returned to automatic status")
}

func back(r *http.Request, def string) string {
	if b := r.PostFormValue("back"); strings.HasPrefix(b, "/") && !strings.HasPrefix(b, "//") {
		return b
	}
	return def
}

// --- incidents ---

func (a *Admin) incidents(w http.ResponseWriter, r *http.Request, s *Session) {
	list, err := a.Store.Incidents(store.IncidentFilter{Limit: 200})
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "incidents", map[string]any{"Title": "Incidents", "List": list})
}

func (a *Admin) incidentForm(w http.ResponseWriter, r *http.Request, s *Session) {
	inc := &store.Incident{Impact: "minor"}
	if c := r.URL.Query().Get("component"); c != "" {
		inc.Components = []string{c}
	}
	a.render(w, r, s, http.StatusOK, "incident", map[string]any{"Title": "New incident", "Inc": inc, "New": true, "Cat": a.Cat})
}

func (a *Admin) incident(w http.ResponseWriter, r *http.Request, s *Session) {
	inc, err := a.Store.Incident(r.PathValue("id"))
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "incident", map[string]any{"Title": inc.Title, "Inc": inc, "Cat": a.Cat})
}

func (a *Admin) validComponents(ids []string) ([]string, error) {
	var out []string
	for _, id := range ids {
		if a.Cat.Component(id) == nil {
			return nil, fmt.Errorf("%w: unknown component %q", store.ErrValidation, id)
		}
		out = append(out, id)
	}
	return out, nil
}

func (a *Admin) saveIncident(w http.ResponseWriter, r *http.Request, s *Session) {
	id := r.PathValue("id")
	isNew := id == ""
	if isNew {
		id = newID("inc")
	}
	comps, err := a.validComponents(r.PostForm["components"])
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	d := store.UpsertIncident{
		ID: id, Title: strings.TrimSpace(r.PostFormValue("title")), Impact: r.PostFormValue("impact"),
		PostmortemURL: strings.TrimSpace(r.PostFormValue("postmortem_url")), Components: comps,
	}
	if d.PostmortemURL != "" && !strings.HasPrefix(d.PostmortemURL, "https://") {
		a.fail(w, r, s, fmt.Errorf("%w: postmortem URL must start with https://", store.ErrValidation))
		return
	}
	if isNew {
		d.FirstUpdate = &store.IncidentUpdate{ID: newID("upd"), State: r.PostFormValue("state"), Body: strings.TrimSpace(r.PostFormValue("body"))}
	}
	if err := d.Validate(); err != nil {
		a.fail(w, r, s, err)
		return
	}
	if err := a.submit(r, s, store.CmdUpsertIncident, d); err != nil {
		a.fail(w, r, s, err)
		return
	}
	msg := "Incident saved"
	if isNew {
		msg = "Incident published"
	}
	a.redirect(w, r, "/incidents/"+id, msg)
}

func (a *Admin) addIncidentUpdate(w http.ResponseWriter, r *http.Request, s *Session) {
	d := store.AddIncidentUpdate{IncidentUpdate: store.IncidentUpdate{
		ID: newID("upd"), IncidentID: r.PathValue("id"), State: r.PostFormValue("state"), Body: strings.TrimSpace(r.PostFormValue("body")),
	}}
	if err := d.Validate(); err != nil {
		a.fail(w, r, s, err)
		return
	}
	if err := a.submit(r, s, store.CmdAddIncidentUpdate, d); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/incidents/"+d.IncidentID, "Update posted")
}

func (a *Admin) deleteIncident(w http.ResponseWriter, r *http.Request, s *Session) {
	if err := a.submit(r, s, store.CmdDeleteIncident, store.DeleteByID{ID: r.PathValue("id")}); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/incidents", "Incident deleted")
}

// --- maintenances ---

func (a *Admin) maintenances(w http.ResponseWriter, r *http.Request, s *Session) {
	list, err := a.Store.Maintenances(store.MaintenanceFilter{IncludeCancelled: true, Limit: 200})
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "maintenances", map[string]any{"Title": "Maintenances", "List": list, "Now": a.now().Unix()})
}

func (a *Admin) maintenanceForm(w http.ResponseWriter, r *http.Request, s *Session) {
	m := &store.Maintenance{}
	isNew := r.PathValue("id") == ""
	if isNew {
		start := a.now().Add(24 * time.Hour).Truncate(time.Hour)
		m.Starts, m.Ends = start.Unix(), start.Add(2*time.Hour).Unix()
	} else {
		var err error
		if m, err = a.Store.Maintenance(r.PathValue("id")); err != nil {
			a.fail(w, r, s, err)
			return
		}
	}
	title := "New maintenance"
	if !isNew {
		title = m.Title
	}
	a.render(w, r, s, http.StatusOK, "maintenance", map[string]any{"Title": title, "M": m, "New": isNew, "Cat": a.Cat, "Now": a.now().Unix()})
}

func parseUTC(v string) (int64, error) {
	t, err := time.ParseInLocation("2006-01-02T15:04", v, time.UTC)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid date/time %q", store.ErrValidation, v)
	}
	return t.Unix(), nil
}

func (a *Admin) saveMaintenance(w http.ResponseWriter, r *http.Request, s *Session) {
	id := r.PathValue("id")
	if id == "" {
		id = newID("mnt")
	}
	starts, err := parseUTC(r.PostFormValue("starts"))
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	ends, err := parseUTC(r.PostFormValue("ends"))
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	comps, err := a.validComponents(r.PostForm["components"])
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	m := store.Maintenance{
		ID: id, Title: strings.TrimSpace(r.PostFormValue("title")), Body: strings.TrimSpace(r.PostFormValue("body")),
		Starts: starts, Ends: ends, Cancelled: r.PostFormValue("cancelled") == "1", Components: comps,
	}
	if err := m.Validate(); err != nil {
		a.fail(w, r, s, err)
		return
	}
	if err := a.submit(r, s, store.CmdUpsertMaintenance, m); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/maintenances/"+id, "Maintenance saved")
}

func (a *Admin) deleteMaintenance(w http.ResponseWriter, r *http.Request, s *Session) {
	if err := a.submit(r, s, store.CmdDeleteMaintenance, store.DeleteByID{ID: r.PathValue("id")}); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/maintenances", "Maintenance deleted")
}

// --- announcements ---

func (a *Admin) announcements(w http.ResponseWriter, r *http.Request, s *Session) {
	list, err := a.Store.Announcements()
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "announcements", map[string]any{"Title": "Announcements", "List": list, "Now": a.now().Unix()})
}

func (a *Admin) announcementForm(w http.ResponseWriter, r *http.Request, s *Session) {
	an := &store.Announcement{Level: "info", Starts: a.now().Truncate(time.Minute).Unix()}
	isNew := r.PathValue("id") == ""
	if !isNew {
		var err error
		if an, err = a.Store.Announcement(r.PathValue("id")); err != nil {
			a.fail(w, r, s, err)
			return
		}
	}
	title := "New announcement"
	if !isNew {
		title = an.Title
	}
	a.render(w, r, s, http.StatusOK, "announcement", map[string]any{"Title": title, "A": an, "New": isNew})
}

func (a *Admin) saveAnnouncement(w http.ResponseWriter, r *http.Request, s *Session) {
	id := r.PathValue("id")
	if id == "" {
		id = newID("ann")
	}
	starts, err := parseUTC(r.PostFormValue("starts"))
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	var ends int64
	if v := r.PostFormValue("ends"); v != "" {
		if ends, err = parseUTC(v); err != nil {
			a.fail(w, r, s, err)
			return
		}
	}
	an := store.Announcement{ID: id, Level: r.PostFormValue("level"), Title: strings.TrimSpace(r.PostFormValue("title")),
		Body: strings.TrimSpace(r.PostFormValue("body")), Starts: starts, Ends: ends}
	if err := an.Validate(); err != nil {
		a.fail(w, r, s, err)
		return
	}
	if err := a.submit(r, s, store.CmdUpsertAnnouncement, an); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/announcements", "Announcement saved")
}

func (a *Admin) deleteAnnouncement(w http.ResponseWriter, r *http.Request, s *Session) {
	if err := a.submit(r, s, store.CmdDeleteAnnouncement, store.DeleteByID{ID: r.PathValue("id")}); err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.redirect(w, r, "/announcements", "Announcement removed")
}

// --- audit, settings ---

func (a *Admin) audit(w http.ResponseWriter, r *http.Request, s *Session) {
	limit := 300
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 5000 {
		limit = v
	}
	list, err := a.Store.Audit(limit)
	if err != nil {
		a.fail(w, r, s, err)
		return
	}
	a.render(w, r, s, http.StatusOK, "audit", map[string]any{"Title": "Audit log", "List": list})
}

func (a *Admin) settings(w http.ResponseWriter, r *http.Request, s *Session) {
	a.render(w, r, s, http.StatusOK, "settings", map[string]any{"Title": "Settings", "Peers": a.Store.Peers(), "State": a.Store.State()})
}

func (a *Admin) saveSettings(w http.ResponseWriter, r *http.Request, s *Session) {
	v := "0"
	if r.PostFormValue("automation_paused") == "1" {
		v = "1"
	}
	if err := a.submit(r, s, store.CmdSetSetting, store.SetSetting{Key: store.SettingAutomationPaused, Value: v}); err != nil {
		a.fail(w, r, s, err)
		return
	}
	msg := "Automation resumed"
	if v == "1" {
		msg = "Automation paused: automatic statuses are frozen"
	}
	a.redirect(w, r, "/settings", msg)
}

func (a *Admin) stateJSON(w http.ResponseWriter, r *http.Request, s *Session) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"cluster": a.Store.State(), "eval": a.Node.EvalStatus(), "health": a.Node.Health()})
}

func newID(prefix string) string {
	return prefix + "_" + strings.ToLower(randomString(9))
}
