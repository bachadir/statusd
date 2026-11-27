package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"statusd/internal/catalog"
	"statusd/internal/model"
	"statusd/internal/view"
)

// Store is the replicated delivery state (implemented over raft in main).
type Store interface {
	IsLeader() bool
	// Barrier waits until this node's state machine has applied every committed
	// entry, so a new leader reads what the previous leader recorded.
	Barrier(timeout time.Duration) error
	WebhookState(webhook string) (map[string]model.Status, error)
	// Record replicates the statuses a webhook now knows about.
	Record(webhook string, statuses map[string]model.Status) error
}

// Change is one component's status change in a delivery.
type Change struct {
	Component ComponentRef `json:"component"`
	From      model.Status `json:"from"`
	To        model.Status `json:"to"`
	Source    model.Source `json:"source"` // auto | manual | maintenance
	Since     time.Time    `json:"since"`  // when the component entered the new status
}

type ComponentRef struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Group string `json:"group"`
}

// Payload is the body of a json-format delivery.
type Payload struct {
	ID      string    `json:"id"`   // stable for the same set of changes: use it to de-duplicate
	Type    string    `json:"type"` // "component.status_changed"
	SentAt  time.Time `json:"sent_at"`
	Page    PageRef   `json:"page"`
	Overall struct {
		Status model.Status `json:"status"`
	} `json:"overall"`
	Changes []Change `json:"changes"`
}

type PageRef struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

// Delivery is one attempt, shown in the control panel.
type Delivery struct {
	At      time.Time `json:"at"`
	Webhook string    `json:"webhook"`
	ID      string    `json:"id"`
	Changes int       `json:"changes"`
	Code    int       `json:"code"`
	Error   string    `json:"error,omitempty"`
}

// HookState is a webhook's delivery state on this node.
type HookState struct {
	Name         string
	Format       string
	Components   string
	Signed       bool
	Pending      int
	LastSuccess  time.Time
	LastError    string
	BackoffUntil time.Time
}

// State is what the control panel shows.
type State struct {
	Active bool // this node is the leader and delivers
	Hooks  []HookState
	Recent []Delivery // newest first
}

type hookRun struct {
	cfg          *Config
	pending      int
	lastSuccess  time.Time
	lastErr      string
	backoff      time.Duration
	backoffUntil time.Time
}

// Dispatcher delivers status changes to every configured webhook.
type Dispatcher struct {
	hooks    []*hookRun
	cat      *catalog.Catalog
	resolve  func(time.Time) (map[string]view.ComponentStatus, error)
	store    Store
	page     PageRef
	client   *http.Client
	log      *slog.Logger
	now      func() time.Time
	every    time.Duration
	maxDelay time.Duration
	version  string

	mu     sync.Mutex
	active bool
	recent []Delivery

	deliveries *prometheus.CounterVec
}

func NewDispatcher(hooks []*Config, cat *catalog.Catalog, resolve func(time.Time) (map[string]view.ComponentStatus, error),
	store Store, page PageRef, version string, log *slog.Logger) *Dispatcher {
	d := &Dispatcher{
		cat: cat, resolve: resolve, store: store, page: page, version: version,
		client: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		log:    log.With("component", "webhook"), now: time.Now, every: 10 * time.Second, maxDelay: 5 * time.Minute,
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "statusd_webhook_deliveries_total", Help: "Webhook delivery attempts by result (ok, error).",
		}, []string{"webhook", "result"}),
	}
	for _, h := range hooks {
		d.hooks = append(d.hooks, &hookRun{cfg: h})
	}
	return d
}

func (d *Dispatcher) RegisterMetrics(reg prometheus.Registerer) {
	pending := prometheus.NewDesc("statusd_webhook_pending", "Component changes not yet delivered (leader only).", []string{"webhook"}, nil)
	last := prometheus.NewDesc("statusd_webhook_last_success_timestamp_seconds", "Last successful delivery.", []string{"webhook"}, nil)
	reg.MustRegister(d.deliveries, collectorFunc{descs: []*prometheus.Desc{pending, last}, collect: func(ch chan<- prometheus.Metric) {
		for _, h := range d.Status().Hooks {
			ch <- prometheus.MustNewConstMetric(pending, prometheus.GaugeValue, float64(h.Pending), h.Name)
			var ts float64
			if !h.LastSuccess.IsZero() {
				ts = float64(h.LastSuccess.Unix())
			}
			ch <- prometheus.MustNewConstMetric(last, prometheus.GaugeValue, ts, h.Name)
		}
	}})
}

type collectorFunc struct {
	descs   []*prometheus.Desc
	collect func(chan<- prometheus.Metric)
}

func (c collectorFunc) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}
func (c collectorFunc) Collect(ch chan<- prometheus.Metric) { c.collect(ch) }

// Run blocks until ctx is done.
func (d *Dispatcher) Run(ctx context.Context) {
	names := make([]string, len(d.hooks))
	for i, h := range d.hooks {
		names[i] = h.cfg.Name
	}
	d.log.Info("webhooks started", "webhooks", strings.Join(names, ","))
	t := time.NewTicker(d.every)
	defer t.Stop()
	for {
		d.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) {
	if !d.store.IsLeader() {
		d.mu.Lock()
		if d.active {
			d.log.Info("not leader: webhooks paused")
		}
		d.active = false
		for _, h := range d.hooks {
			h.pending, h.backoff, h.backoffUntil = 0, 0, time.Time{}
		}
		d.mu.Unlock()
		return
	}
	d.mu.Lock()
	wasActive := d.active
	d.mu.Unlock()
	if !wasActive {
		if err := d.store.Barrier(10 * time.Second); err != nil {
			d.log.Warn("barrier before delivering webhooks", "err", err)
			return
		}
		d.mu.Lock()
		d.active = true
		d.mu.Unlock()
		d.log.Info("leader: delivering webhooks")
	}
	d.Sync(ctx)
}

// Sync delivers pending changes to every webhook once (leader only).
func (d *Dispatcher) Sync(ctx context.Context) {
	now := d.now()
	cur, err := d.resolve(now)
	if err != nil {
		d.log.Warn("resolve statuses", "err", err)
		return
	}
	overall := model.Operational
	for _, c := range d.cat.Components() {
		if !c.Hidden {
			overall = model.Worst(overall, cur[c.ID].Status)
		}
	}
	for _, h := range d.hooks {
		if ctx.Err() != nil {
			return
		}
		d.syncHook(ctx, h, cur, overall, now)
	}
}

func (d *Dispatcher) syncHook(ctx context.Context, h *hookRun, cur map[string]view.ComponentStatus, overall model.Status, now time.Time) {
	known, err := d.store.WebhookState(h.cfg.Name)
	if err != nil {
		d.log.Warn("read webhook state", "webhook", h.cfg.Name, "err", err)
		return
	}
	baseline := map[string]model.Status{} // components this webhook has never seen: recorded silently
	skipped := map[string]model.Status{}  // changes below min_status: recorded without sending
	var send []Change
	for _, c := range d.cat.Components() {
		if c.Hidden || !h.cfg.Matches(c.ID) {
			continue
		}
		cs, ok := cur[c.ID]
		if !ok {
			continue
		}
		prev, seen := known[c.ID]
		switch {
		case !seen:
			baseline[c.ID] = cs.Status
		case prev != cs.Status:
			if min := model.Status(h.cfg.MinStatus); min != "" && prev.Severity() < min.Severity() && cs.Status.Severity() < min.Severity() {
				skipped[c.ID] = cs.Status
				continue
			}
			send = append(send, Change{
				Component: ComponentRef{ID: c.ID, Name: c.Name, Group: c.Group.Name},
				From:      prev, To: cs.Status, Source: cs.Source, Since: time.Unix(cs.Since, 0).UTC(),
			})
		}
	}
	for id, st := range skipped {
		baseline[id] = st
	}
	if len(baseline) > 0 {
		if err := d.store.Record(h.cfg.Name, baseline); err != nil {
			d.log.Warn("record webhook baseline", "webhook", h.cfg.Name, "err", err)
			return
		}
	}
	d.mu.Lock()
	h.pending = len(send)
	wait := now.Before(h.backoffUntil)
	d.mu.Unlock()
	if len(send) == 0 || wait {
		return
	}
	sort.Slice(send, func(i, j int) bool { return send[i].Component.ID < send[j].Component.ID })

	id := eventID(h.cfg.Name, send)
	body, err := d.body(h.cfg, id, send, overall, now)
	if err != nil {
		d.log.Error("build webhook body", "webhook", h.cfg.Name, "err", err)
		return
	}
	code, err := d.post(ctx, h.cfg, id, body, now)
	del := Delivery{At: now, Webhook: h.cfg.Name, ID: id, Changes: len(send), Code: code}
	if err != nil {
		del.Error = err.Error()
		d.deliveries.WithLabelValues(h.cfg.Name, "error").Inc()
		d.mu.Lock()
		h.lastErr = del.Error
		if h.backoff == 0 {
			h.backoff = 15 * time.Second
		} else if h.backoff *= 2; h.backoff > d.maxDelay {
			h.backoff = d.maxDelay
		}
		h.backoffUntil = now.Add(h.backoff)
		d.addRecent(del)
		d.mu.Unlock()
		d.log.Warn("webhook delivery failed", "webhook", h.cfg.Name, "id", id, "changes", len(send), "code", code, "err", err, "retry_in", h.backoff)
		return
	}
	d.deliveries.WithLabelValues(h.cfg.Name, "ok").Inc()
	delivered := map[string]model.Status{}
	for _, c := range send {
		delivered[c.Component.ID] = c.To
	}
	if err := d.store.Record(h.cfg.Name, delivered); err != nil {
		// Delivered but not recorded (leadership lost?): the next leader sends the
		// same event again, with the same id.
		d.log.Warn("record webhook delivery", "webhook", h.cfg.Name, "id", id, "err", err)
	}
	d.mu.Lock()
	h.pending, h.lastErr, h.backoff, h.backoffUntil, h.lastSuccess = 0, "", 0, time.Time{}, now
	d.addRecent(del)
	d.mu.Unlock()
	for _, c := range send {
		d.log.Info("webhook delivered", "webhook", h.cfg.Name, "id", id, "component_id", c.Component.ID, "from", c.From, "to", c.To)
	}
}

func (d *Dispatcher) addRecent(del Delivery) {
	d.recent = append([]Delivery{del}, d.recent...)
	if len(d.recent) > 50 {
		d.recent = d.recent[:50]
	}
}

// eventID is derived from the changes, so a retry or a new leader sending the
// same changes uses the same id.
func eventID(hook string, cs []Change) string {
	h := sha256.New()
	h.Write([]byte(hook))
	for _, c := range cs {
		fmt.Fprintf(h, "\x00%s\x00%s\x00%s\x00%d", c.Component.ID, c.From, c.To, c.Since.Unix())
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func (d *Dispatcher) body(h *Config, id string, cs []Change, overall model.Status, now time.Time) ([]byte, error) {
	if h.Format == FormatSlack {
		return json.Marshal(map[string]string{"text": slackText(d.page, cs, overall)})
	}
	p := Payload{ID: id, Type: "component.status_changed", SentAt: now.UTC(), Page: d.page, Changes: cs}
	p.Overall.Status = overall
	return json.Marshal(p)
}

var emoji = map[model.Status]string{
	model.Operational:         ":large_green_circle:",
	model.UnderMaintenance:    ":large_blue_circle:",
	model.DegradedPerformance: ":large_yellow_circle:",
	model.PartialOutage:       ":large_orange_circle:",
	model.MajorOutage:         ":red_circle:",
}

func slackText(page PageRef, cs []Change, overall model.Status) string {
	var b strings.Builder
	name := page.Name
	if page.URL != "" {
		name = "<" + page.URL + "|" + slackEscape(page.Name) + ">"
	} else {
		name = slackEscape(name)
	}
	fmt.Fprintf(&b, "*%s*: %s %s\n", name, emoji[overall], overall.Label())
	for _, c := range cs {
		src := ""
		switch c.Source {
		case model.SourceManual:
			src = " (set by staff)"
		case model.SourceMaintenance:
			src = " (maintenance)"
		}
		fmt.Fprintf(&b, "%s *%s* (%s): %s → %s%s\n", emoji[c.To], slackEscape(c.Component.Name), slackEscape(c.Component.Group),
			c.From.Label(), c.To.Label(), src)
	}
	return strings.TrimRight(b.String(), "\n")
}

func slackEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

func (d *Dispatcher) post(ctx context.Context, h *Config, id string, body []byte, now time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, h.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "statusd/"+d.version)
	if h.Format == FormatJSON {
		ts := strconv.FormatInt(now.Unix(), 10)
		req.Header.Set("X-Statusd-Event", "component.status_changed")
		req.Header.Set("X-Statusd-Delivery", id)
		req.Header.Set("X-Statusd-Timestamp", ts)
		if h.Signed() {
			req.Header.Set("X-Statusd-Signature", "sha256="+Sign(h.secret, ts, body))
		}
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, redact(err, h.url)
	}
	defer resp.Body.Close()
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return resp.StatusCode, nil
}

// Sign is the signature receivers check: hex HMAC-SHA256 over "<timestamp>.<body>",
// with the timestamp from X-Statusd-Timestamp (reject old timestamps to stop replays).
func Sign(secret []byte, ts string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(ts + "."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

// redact keeps webhook URLs (often secret, e.g. Slack's) out of logs and the panel.
func redact(err error, u string) error {
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), u, "<webhook url>"))
}

// Status is a snapshot for the control panel and metrics.
func (d *Dispatcher) Status() State {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := State{Active: d.active, Recent: append([]Delivery(nil), d.recent...)}
	for _, h := range d.hooks {
		st.Hooks = append(st.Hooks, HookState{
			Name: h.cfg.Name, Format: h.cfg.Format, Components: h.cfg.Components, Signed: h.cfg.Signed(),
			Pending: h.pending, LastSuccess: h.lastSuccess, LastError: h.lastErr, BackoffUntil: h.backoffUntil,
		})
	}
	return st
}
