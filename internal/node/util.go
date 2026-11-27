package node

import (
	"crypto/subtle"

	"github.com/prometheus/client_golang/prometheus"
)

func subtleEq(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// RegisterMetrics exposes cluster and evaluation health for Alertmanager.
func (n *Node) RegisterMetrics(reg prometheus.Registerer) {
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_raft_leader", Help: "1 if this node is the raft leader."},
			func() float64 { return b2f(n.Store.IsLeader()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_raft_has_leader", Help: "1 if this node knows a raft leader."},
			func() float64 { return b2f(n.Store.Leader() != "") }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_last_tick_timestamp_seconds", Help: "Timestamp of the newest applied evaluation tick."},
			func() float64 { return float64(n.Store.LastTick()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_witness", Help: "1 if this node is a witness."},
			func() float64 { return b2f(n.cfg.Witness) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "statusd_ticks_total", Help: "Evaluation ticks run on this node while leader."},
			func() float64 { return float64(n.EvalStatus().Ticks) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "statusd_tick_errors_total", Help: "Evaluation ticks that failed to commit."},
			func() float64 { return float64(n.EvalStatus().TickErrors) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "statusd_last_eval_duration_seconds", Help: "Duration of the last evaluation."},
			func() float64 { return n.EvalStatus().LastEvalDur.Seconds() }),
		&sourceCollector{n: n},
	)
}

type sourceCollector struct{ n *Node }

var (
	descSourceUp = prometheus.NewDesc("statusd_source_healthy", "1 if the Prometheus source is reachable from this node.", []string{"source"}, nil)
	descSourceOK = prometheus.NewDesc("statusd_source_last_success_timestamp_seconds", "Last successful query per source.", []string{"source"}, nil)
)

func (c *sourceCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descSourceUp
	ch <- descSourceOK
}

func (c *sourceCollector) Collect(ch chan<- prometheus.Metric) {
	if c.n.Col == nil {
		return
	}
	for _, h := range c.n.Col.Health() {
		ch <- prometheus.MustNewConstMetric(descSourceUp, prometheus.GaugeValue, b2f(h.Healthy), h.Source)
		var ts float64
		if !h.LastOK.IsZero() {
			ts = float64(h.LastOK.UnixMilli()) / 1000
		}
		ch <- prometheus.MustNewConstMetric(descSourceOK, prometheus.GaugeValue, ts, h.Source)
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
