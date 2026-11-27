package statusio

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"statusd/internal/catalog"
	"statusd/internal/model"
	"statusd/internal/view"
)

// Publisher pushes the published status of every mapped catalogue component
// to Status.io. It only works while this node is the raft leader; a new leader
// first reads Status.io's summary, so nothing is double-posted or missed.
//
// Status.io itself is the "last pushed" state: nothing about the pushes is
// kept in raft.
type Publisher struct {
	cfg      Config
	client   *Client
	cat      *catalog.Catalog
	resolve  func(time.Time) (map[string]view.ComponentStatus, error)
	isLeader func() bool
	log      *slog.Logger
	now      func() time.Time

	mu            sync.Mutex
	active        bool         // leader and running
	known         map[Pair]int // what Status.io shows; nil = must reconcile
	pushed        map[Pair]int // what this leader pushed (drift detection)
	rejected      map[Pair]int // desired code Status.io refused; not retried until it changes
	lastReconcile time.Time
	backoffUntil  time.Time
	backoff       time.Duration
	lastSuccess   time.Time
	lastErr       string
	recent        []Push

	requests *prometheus.CounterVec
	drift    prometheus.Counter
}

// Push is one status sent (or, in dry run, logged) to Status.io.
type Push struct {
	At        time.Time    `json:"at"`
	Component string       `json:"component"` // catalogue component ID
	Pair      Pair         `json:"pair"`
	Status    model.Status `json:"status"`
	Code      int          `json:"code"`
	DryRun    bool         `json:"dry_run"`
	Error     string       `json:"error,omitempty"`
}

func NewPublisher(cfg Config, client *Client, cat *catalog.Catalog,
	resolve func(time.Time) (map[string]view.ComponentStatus, error), isLeader func() bool, log *slog.Logger) *Publisher {
	return &Publisher{
		cfg: cfg, client: client, cat: cat, resolve: resolve, isLeader: isLeader,
		log: log.With("component", "statusio", "dry_run", cfg.IsDryRun()), now: time.Now,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "statusd_statusio_requests_total", Help: "Requests to the Status.io API (dry_run: logged only).",
		}, []string{"endpoint", "result"}),
		drift: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "statusd_statusio_drift_total", Help: "Pairs found changed on Status.io since this node pushed them (hand edits).",
		}),
	}
}

func (p *Publisher) RegisterMetrics(reg prometheus.Registerer) {
	reg.MustRegister(p.requests, p.drift,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_statusio_pending", Help: "Mapped pairs whose Status.io status differs from statusd's (leader only)."},
			func() float64 { return float64(p.Status().Pending) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_statusio_last_success_timestamp_seconds", Help: "Last successful Status.io request."},
			func() float64 {
				if t := p.Status().LastSuccess; !t.IsZero() {
					return float64(t.Unix())
				}
				return 0
			}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_statusio_dry_run", Help: "1 if Status.io pushes are only logged."},
			func() float64 {
				if p.cfg.IsDryRun() {
					return 1
				}
				return 0
			}),
	)
}

// Run blocks until ctx is done.
func (p *Publisher) Run(ctx context.Context) {
	p.log.Info("status.io publisher started", "statuspage", p.cfg.StatuspageID, "mapped", len(p.mapping()))
	t := time.NewTicker(p.cfg.SyncEvery)
	defer t.Stop()
	for {
		if p.isLeader() {
			p.mu.Lock()
			if !p.active {
				p.active, p.known, p.pushed = true, nil, map[Pair]int{}
				p.log.Info("leader: syncing statuses to status.io")
			}
			p.mu.Unlock()
			p.Sync(ctx)
		} else {
			p.mu.Lock()
			p.active, p.known, p.pushed = false, nil, nil
			p.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

type target struct {
	id     string // catalogue component ID
	pair   Pair
	status model.Status
	code   int
}

func (p *Publisher) mapping() map[string]Pair {
	out := map[string]Pair{}
	for _, g := range p.cat.Groups {
		if g.Hidden {
			continue
		}
		for _, c := range g.Components {
			if c.StatusIO != nil && !c.Hidden {
				out[c.ID] = Pair{Component: c.StatusIO.Component, Container: c.StatusIO.Container}
			}
		}
	}
	return out
}

func (p *Publisher) desired(now time.Time) ([]target, error) {
	res, err := p.resolve(now)
	if err != nil {
		return nil, err
	}
	var out []target
	for id, pair := range p.mapping() {
		st := model.Operational
		if r, ok := res[id]; ok && r.Status != "" {
			st = r.Status
		}
		out = append(out, target{id: id, pair: pair, status: st, code: Code(st)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

// Sync reconciles if due, then pushes every pair whose status differs.
func (p *Publisher) Sync(ctx context.Context) {
	now := p.now()
	p.mu.Lock()
	inBackoff := now.Before(p.backoffUntil)
	needReconcile := p.known == nil || now.Sub(p.lastReconcile) >= p.cfg.ReconcileEvery
	p.mu.Unlock()
	if inBackoff {
		return
	}
	if needReconcile && !p.reconcile(ctx) {
		return
	}
	want, err := p.desired(now)
	if err != nil {
		p.log.Error("resolve statuses", "err", err)
		return
	}
	for _, t := range want {
		if ctx.Err() != nil || !p.isLeader() {
			return
		}
		p.mu.Lock()
		cur, seen := p.known[t.pair]
		rej, wasRejected := p.rejected[t.pair]
		p.mu.Unlock()
		if (seen && cur == t.code) || (wasRejected && rej == t.code) {
			continue
		}
		if !p.push(ctx, t, cur, seen) {
			return // transient failure: back off, retry everything later
		}
	}
}

// reconcile reads Status.io's summary as the known state.
func (p *Publisher) reconcile(ctx context.Context) bool {
	if !p.client.HasCredentials() {
		// Dry run without credentials: nothing to read; assume nothing is known,
		// so every mapped pair is logged once.
		p.mu.Lock()
		if p.known == nil {
			p.known = map[Pair]int{}
		}
		p.lastReconcile = p.now()
		p.mu.Unlock()
		return true
	}
	sum, err := p.client.Summary(ctx)
	p.count("status/summary", err)
	if err != nil {
		p.fail("read status.io summary", err)
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for pair, pushed := range p.pushed {
		if cur, ok := sum[pair]; ok && cur != pushed {
			p.drift.Inc()
			p.log.Warn("status.io was changed outside statusd; overwriting",
				"pair", pair.String(), "pushed", pushed, "found", cur)
		}
	}
	for id, pair := range p.mapping() {
		if _, ok := sum[pair]; !ok {
			p.log.Warn("mapped pair is not on the status.io page", "component", id, "pair", pair.String())
		}
	}
	p.known, p.lastReconcile = sum, p.now()
	return true
}

func (p *Publisher) push(ctx context.Context, t target, cur int, seen bool) bool {
	details := p.cfg.Details[t.status]
	if details == "" {
		details = DefaultDetails[t.status]
	}
	dry := p.cfg.IsDryRun()
	var err error
	if !dry {
		err = p.client.UpdateStatus(ctx, t.pair, t.code, details)
	}
	if dry {
		p.requests.WithLabelValues("component/status/update", "dry_run").Inc()
	} else {
		p.count("component/status/update", err)
	}
	from := any(cur)
	if !seen {
		from = "unknown"
	}
	push := Push{At: p.now(), Component: t.id, Pair: t.pair, Status: t.status, Code: t.code, DryRun: dry}
	if err != nil {
		push.Error = err.Error()
	}
	p.record(push)
	switch {
	case err == nil:
		p.log.Info("status.io status pushed", "component", t.id, "pair", t.pair.String(),
			"from", from, "to", t.code, "status", t.status, "details", details)
		p.mu.Lock()
		p.known[t.pair] = t.code
		if p.pushed != nil && !dry { // dry-run "pushes" never reached Status.io
			p.pushed[t.pair] = t.code
		}
		delete(p.rejected, t.pair)
		p.mu.Unlock()
		return true
	case IsPermanent(err):
		p.log.Error("status.io rejected the status; not retrying until it changes",
			"component", t.id, "pair", t.pair.String(), "to", t.code, "err", err)
		p.mu.Lock()
		if p.rejected == nil {
			p.rejected = map[Pair]int{}
		}
		p.rejected[t.pair] = t.code
		p.lastErr = err.Error()
		p.mu.Unlock()
		return true
	default:
		p.fail("push status to status.io", err)
		return false
	}
}

func (p *Publisher) count(endpoint string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	p.requests.WithLabelValues(endpoint, result).Inc()
	if err == nil {
		p.mu.Lock()
		p.lastSuccess, p.backoff, p.lastErr = p.now(), 0, ""
		p.mu.Unlock()
	}
}

// fail backs off after a transient error: 15 s, doubling up to 5 min.
func (p *Publisher) fail(what string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backoff = min(max(2*p.backoff, 15*time.Second), 5*time.Minute)
	p.backoffUntil = p.now().Add(p.backoff)
	p.lastErr = err.Error()
	p.log.Warn(what, "err", err, "retry_in", p.backoff)
}

func (p *Publisher) record(x Push) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recent = append(p.recent, x)
	if n := len(p.recent); n > 50 {
		p.recent = p.recent[n-50:]
	}
}

// State is shown in the control panel.
type State struct {
	Enabled       bool      `json:"enabled"`
	DryRun        bool      `json:"dry_run"`
	StatuspageID  string    `json:"statuspage_id"`
	Active        bool      `json:"active"` // this node is the leader and publishing
	Mapped        int       `json:"mapped"`
	Pending       int       `json:"pending"`
	LastReconcile time.Time `json:"last_reconcile"`
	LastSuccess   time.Time `json:"last_success"`
	LastError     string    `json:"last_error,omitempty"`
	Recent        []Push    `json:"recent"` // newest first
}

func (p *Publisher) Status() State {
	m := p.mapping()
	var want []target
	p.mu.Lock()
	active := p.active && p.known != nil
	p.mu.Unlock()
	if active {
		want, _ = p.desired(p.now())
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := State{
		Enabled: true, DryRun: p.cfg.IsDryRun(), StatuspageID: p.cfg.StatuspageID, Active: p.active,
		Mapped: len(m), LastReconcile: p.lastReconcile, LastSuccess: p.lastSuccess, LastError: p.lastErr,
	}
	for _, t := range want {
		if cur, ok := p.known[t.pair]; !ok || cur != t.code {
			if rej, ok := p.rejected[t.pair]; !ok || rej != t.code {
				s.Pending++
			}
		}
	}
	for i := len(p.recent) - 1; i >= 0; i-- {
		s.Recent = append(s.Recent, p.recent[i])
	}
	return s
}
