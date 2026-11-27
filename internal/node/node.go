// Package node runs one statusd cluster member: the evaluation loop while it
// is the raft leader, write submission (applied locally on the leader,
// forwarded otherwise), and source-aware leadership transfer.
package node

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/collector"
	"statusd/internal/eval"
	"statusd/internal/model"
	"statusd/internal/store"
)

type Config struct {
	Witness        bool
	Interval       time.Duration // evaluation interval (default 60s)
	RetentionDays  int           // uptime history kept (default 90)
	ClusterKey     string        // shared secret for the internal API
	ClientTLS      *tls.Config   // mTLS for forwarding (nil = plain HTTP, internal networks only)
	ApplyTimeout   time.Duration
	TransferPolicy bool // enable source-aware leadership transfer (default true via config)
}

type Node struct {
	cfg   Config
	Store *store.Store
	Col   *collector.Collector
	Cat   *catalog.Catalog
	eval  *eval.Evaluator
	log   *slog.Logger
	http  *http.Client

	mu            sync.Mutex
	lastEvalAt    time.Time
	lastEvalDur   time.Duration
	lastEvalErr   string
	badSourceRuns int
	lastTransfer  time.Time
	ticks         uint64
	tickErrors    uint64
}

func New(cfg Config, st *store.Store, col *collector.Collector, cat *catalog.Catalog, log *slog.Logger) *Node {
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = 90
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = 10 * time.Second
	}
	n := &Node{cfg: cfg, Store: st, Col: col, Cat: cat, log: log}
	if col != nil && cat != nil {
		n.eval = eval.New(cat, col)
	}
	var ctls *tls.Config
	if cfg.ClientTLS != nil {
		ctls = cfg.ClientTLS.Clone()
		ctls.NextProtos = nil // HTTP/1.1 only; never inherit the server's ALPN list
	}
	tr := &http.Transport{TLSClientConfig: ctls, MaxIdleConnsPerHost: 2}
	n.http = &http.Client{Timeout: 15 * time.Second, Transport: tr}
	return n
}

func (n *Node) Interval() time.Duration { return n.cfg.Interval }
func (n *Node) Witness() bool           { return n.cfg.Witness }

// Run blocks until ctx is done.
func (n *Node) Run(ctx context.Context) {
	var cancelLeader context.CancelFunc
	stopLeader := func() {
		if cancelLeader != nil {
			cancelLeader()
			cancelLeader = nil
		}
	}
	defer stopLeader()
	leaderCh := n.Store.LeaderCh()
	// LeaderCh only reports changes; check the current state once.
	isLeader := n.Store.IsLeader()
	for {
		if isLeader && cancelLeader == nil {
			lctx, cancel := context.WithCancel(ctx)
			cancelLeader = cancel
			go n.leaderLoop(lctx)
		} else if !isLeader {
			stopLeader()
		}
		select {
		case <-ctx.Done():
			return
		case isLeader = <-leaderCh:
			n.log.Info("leadership changed", "leader", isLeader, "node", n.Store.NodeID())
		}
	}
}

func (n *Node) leaderLoop(ctx context.Context) {
	if n.cfg.Witness {
		n.log.Warn("witness became leader; handing leadership to a full node")
		for ctx.Err() == nil {
			for _, p := range n.Store.Peers() {
				if p.Witness || p.ID == n.Store.NodeID() {
					continue
				}
				// Only hand over to a peer that answers: a transfer to a dead
				// node blocks for the whole election timeout.
				var hr HealthReport
				if err := n.internalGet(ctx, p.API+"/internal/v1/health", &hr); err != nil {
					n.log.Info("skipping unreachable peer for handover", "peer", p.ID, "err", err)
					continue
				}
				if err := n.Store.TransferLeadership(p.ID); err == nil {
					n.log.Info("leadership handed over", "to", p.ID)
					return
				} else {
					n.log.Warn("leadership transfer failed", "to", p.ID, "err", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
		return
	}
	// Make sure this node has applied everything committed before taking over.
	if err := n.Store.Barrier(30 * time.Second); err != nil {
		n.log.Error("barrier failed", "err", err)
	}
	states, err := n.Store.Components()
	if err == nil {
		seed := map[string]model.Level{}
		for id, cs := range states {
			seed[id] = cs.AutoLevel
		}
		n.eval.Seed(seed)
	}
	n.log.Info("leader: starting evaluation loop", "interval", n.cfg.Interval)

	// Align ticks to the interval so leadership changes keep a steady cadence.
	next := time.Now().Truncate(n.cfg.Interval).Add(n.cfg.Interval)
	if last := n.Store.LastTick(); last == 0 || time.Since(time.Unix(last, 0)) > n.cfg.Interval {
		next = time.Now() // evaluate right away after a gap
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Until(next)):
		}
		n.tick(ctx)
		n.maybeTransfer(ctx)
		next = time.Now().Truncate(n.cfg.Interval).Add(n.cfg.Interval)
	}
}

func (n *Node) tick(ctx context.Context) {
	start := time.Now()
	now := start.Truncate(time.Second)
	results := n.eval.Evaluate(ctx, now)
	if ctx.Err() != nil {
		return
	}
	tick := store.RecordTick{Node: n.Store.NodeID(), PruneDays: n.cfg.RetentionDays + 1}
	interval := n.cfg.Interval.Seconds()
	if last := n.Store.LastTick(); last > 0 {
		gap := float64(now.Unix() - last)
		if gap <= 0 {
			return // already recorded this second
		}
		// A long gap (no leader) is not attributed to any status.
		interval = min(gap, 2*n.cfg.Interval.Seconds())
	}
	tick.Interval = interval
	for _, r := range results {
		tc := store.TickComponent{ID: r.ID, Raw: r.Raw, Auto: r.Auto, AutoStatus: r.Status, Automated: r.Automated}
		if len(r.Signals) > 0 {
			tc.Signals, _ = json.Marshal(r.Signals)
		}
		if r.Freshness != nil {
			age, lvl := r.Freshness.AgeSeconds, r.Freshness.Level
			tc.FreshAge, tc.FreshLevel = &age, &lvl
		}
		tick.Components = append(tick.Components, tc)
	}
	cmd, err := store.NewCommand(store.CmdRecordTick, now.Unix(), "", tick)
	if err == nil {
		err = n.Store.Apply(cmd, n.cfg.ApplyTimeout)
	}
	n.mu.Lock()
	n.lastEvalAt, n.lastEvalDur = start, time.Since(start)
	n.ticks++
	if err != nil {
		n.tickErrors++
		n.lastEvalErr = err.Error()
	} else {
		n.lastEvalErr = ""
	}
	n.mu.Unlock()
	if err != nil {
		n.log.Error("record tick failed", "err", err)
	}
}

// --- source-aware leadership ---

// HealthReport is what a node tells its peers about its view of the sources.
type HealthReport struct {
	Node           string             `json:"node"`
	Witness        bool               `json:"witness"`
	HealthySources int                `json:"healthy_sources"`
	TotalSources   int                `json:"total_sources"`
	Sources        []collector.Health `json:"sources"`
}

func (n *Node) Health() HealthReport {
	r := HealthReport{Node: n.Store.NodeID(), Witness: n.cfg.Witness}
	if n.Col == nil {
		return r
	}
	r.Sources = n.Col.Health()
	r.TotalSources = len(r.Sources)
	for _, h := range r.Sources {
		if h.Healthy {
			r.HealthySources++
		}
	}
	return r
}

// ProbeSources makes followers keep their source health current (they don't
// evaluate, so they'd otherwise never query). Cheap: one `vector(1)` per source.
func (n *Node) ProbeSources(ctx context.Context) {
	if n.Col == nil || n.cfg.Witness {
		return
	}
	t := time.NewTicker(n.cfg.Interval)
	defer t.Stop()
	for {
		if !n.Store.IsLeader() {
			for src := range n.Col.Sources() {
				qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, _ = n.Col.Query(qctx, src, "vector(1)", time.Now())
				cancel()
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (n *Node) maybeTransfer(ctx context.Context) {
	if !n.cfg.TransferPolicy {
		return
	}
	mine := n.Health()
	if mine.HealthySources == mine.TotalSources {
		n.badSourceRuns = 0
		return
	}
	n.badSourceRuns++
	if n.badSourceRuns < 3 || time.Since(n.lastTransfer) < 10*time.Minute {
		return
	}
	for _, p := range n.Store.Peers() {
		if p.Witness || p.ID == n.Store.NodeID() || p.API == "" {
			continue
		}
		var theirs HealthReport
		if err := n.internalGet(ctx, p.API+"/internal/v1/health", &theirs); err != nil {
			n.log.Warn("peer health check failed", "peer", p.ID, "err", err)
			continue
		}
		if theirs.HealthySources > mine.HealthySources {
			n.log.Warn("peer sees more sources; transferring leadership",
				"peer", p.ID, "mine", mine.HealthySources, "theirs", theirs.HealthySources)
			n.lastTransfer = time.Now()
			n.badSourceRuns = 0
			if err := n.Store.TransferLeadership(p.ID); err != nil {
				n.log.Error("leadership transfer failed", "err", err)
			}
			return
		}
	}
}

// --- writes ---

// ErrNoLeader means no leader is reachable: the cluster has lost quorum or is
// electing. Writes must be retried.
var ErrNoLeader = errors.New("no raft leader available; try again shortly")

// Submit applies a command on the leader, forwarding it if this node is a
// follower.
func (n *Node) Submit(ctx context.Context, cmd store.Command) error {
	if n.Store.IsLeader() {
		return n.Store.Apply(cmd, n.cfg.ApplyTimeout)
	}
	leader := n.Store.Leader()
	if leader == "" {
		return ErrNoLeader
	}
	p, ok := n.Store.Peer(leader)
	if !ok || p.API == "" {
		return fmt.Errorf("leader %s has no internal API address configured", leader)
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.API+"/internal/v1/apply", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Statusd-Cluster-Key", n.cfg.ClusterKey)
	resp, err := n.http.Do(req)
	if err != nil {
		return fmt.Errorf("forward to leader %s: %w", leader, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnprocessableEntity:
		return fmt.Errorf("%w: %s", store.ErrValidation, bytes.TrimSpace(msg))
	case http.StatusServiceUnavailable:
		return ErrNoLeader
	}
	return fmt.Errorf("leader %s: HTTP %d: %s", leader, resp.StatusCode, bytes.TrimSpace(msg))
}

func (n *Node) internalGet(ctx context.Context, url string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Statusd-Cluster-Key", n.cfg.ClusterKey)
	resp, err := n.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// EvalStatus is the leader's evaluation loop status (zero on followers).
type EvalStatus struct {
	LastEvalAt  time.Time     `json:"last_eval_at"`
	LastEvalDur time.Duration `json:"last_eval_duration"`
	LastError   string        `json:"last_error,omitempty"`
	Ticks       uint64        `json:"ticks"`
	TickErrors  uint64        `json:"tick_errors"`
}

func (n *Node) EvalStatus() EvalStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	return EvalStatus{LastEvalAt: n.lastEvalAt, LastEvalDur: n.lastEvalDur, LastError: n.lastEvalErr, Ticks: n.ticks, TickErrors: n.tickErrors}
}

// InternalHandler serves the cluster-internal API (write forwarding, health).
func (n *Node) InternalHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/v1/apply", func(w http.ResponseWriter, r *http.Request) {
		if !n.authorized(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = n.Store.ApplyRaw(body, n.cfg.ApplyTimeout)
		switch {
		case err == nil:
			w.WriteHeader(http.StatusOK)
		case errors.Is(err, store.ErrValidation):
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		case errors.Is(err, store.ErrNotLeader):
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("GET /internal/v1/health", func(w http.ResponseWriter, r *http.Request) {
		if !n.authorized(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(n.Health())
	})
	return mux
}

func (n *Node) authorized(r *http.Request) bool {
	if n.cfg.ClusterKey == "" {
		return true
	}
	return subtleEq(r.Header.Get("X-Statusd-Cluster-Key"), n.cfg.ClusterKey)
}
