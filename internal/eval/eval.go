// Package eval turns catalogue signals into component levels: it runs the
// queries, votes probe results across regions, and applies hysteresis.
package eval

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/collector"
	"statusd/internal/model"
)

// Querier is the subset of the collector the evaluator needs.
type Querier interface {
	Query(ctx context.Context, source, expr string, ts time.Time) ([]collector.Sample, error)
}

// SignalResult is the outcome of one signal for one evaluation.
type SignalResult struct {
	Name   string      `json:"name"`
	Kind   string      `json:"kind"`
	Level  model.Level `json:"level"`
	Value  *float64    `json:"value,omitempty"`
	Detail string      `json:"detail,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// FreshnessResult is the data age for a component.
type FreshnessResult struct {
	AgeSeconds float64     `json:"age_seconds"`
	Level      model.Level `json:"level"`
}

// Result is the evaluation of one component.
type Result struct {
	ID        string           `json:"id"`
	Raw       model.Level      `json:"raw"`  // this evaluation, before hysteresis
	Auto      model.Level      `json:"auto"` // published automatic level, after hysteresis
	Status    model.Status     `json:"status"`
	Automated bool             `json:"automated"`
	Signals   []SignalResult   `json:"signals,omitempty"`
	Freshness *FreshnessResult `json:"freshness,omitempty"`
}

type Evaluator struct {
	cat     *catalog.Catalog
	q       Querier
	timeout time.Duration
	maxPar  int

	mu   sync.Mutex
	hyst map[string]*Hysteresis
}

func New(cat *catalog.Catalog, q Querier) *Evaluator {
	return &Evaluator{cat: cat, q: q, timeout: 20 * time.Second, maxPar: 8, hyst: map[string]*Hysteresis{}}
}

// Seed initialises hysteresis from the last published levels (e.g. after a
// leadership change) so a new leader does not re-publish from scratch.
func (e *Evaluator) Seed(levels map[string]model.Level) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hyst = map[string]*Hysteresis{}
	for id, l := range levels {
		if l >= model.Down {
			e.hyst[id] = &Hysteresis{pub: l, init: true}
		}
	}
}

// Evaluate runs every component's signals at ts.
func (e *Evaluator) Evaluate(ctx context.Context, ts time.Time) []Result {
	comps := e.cat.Components()
	out := make([]Result, len(comps))
	sem := make(chan struct{}, e.maxPar)
	var wg sync.WaitGroup
	for i, c := range comps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = e.evalComponent(ctx, c, ts)
		}()
	}
	wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range out {
		c := comps[i]
		r := &out[i]
		if !r.Automated {
			r.Auto = model.OK
		} else {
			h := e.hyst[c.ID]
			if h == nil {
				h = &Hysteresis{}
				e.hyst[c.ID] = h
			}
			r.Auto = h.Step(r.Raw, c.Hysteresis.DownAfter, c.Hysteresis.UpAfter)
		}
		r.Status = c.Mapping().StatusFor(r.Auto)
	}
	return out
}

func (e *Evaluator) evalComponent(ctx context.Context, c *catalog.Component, ts time.Time) Result {
	r := Result{ID: c.ID, Automated: c.Automated(), Raw: model.NoData}
	levels := []model.Level{}
	for _, s := range c.Signals {
		sr := e.evalSignal(ctx, s, ts)
		r.Signals = append(r.Signals, sr)
		levels = append(levels, sr.Level)
	}
	if f := c.Freshness; f != nil {
		fr, sr := e.evalFreshness(ctx, f, ts)
		r.Freshness = fr
		if f.Affects() {
			levels = append(levels, sr.Level)
		} else {
			// Still listed so staff can see it, but it never changes the status.
			sr.Kind = "freshness (display only)"
		}
		r.Signals = append(r.Signals, sr)
	}
	if !r.Automated {
		r.Raw = model.OK
		return r
	}
	r.Raw = Combine(levels)
	return r
}

// Combine returns the worst known level; NoData only if every signal is NoData.
func Combine(levels []model.Level) model.Level {
	out := model.NoData
	for _, l := range levels {
		if l == model.NoData {
			continue
		}
		if out == model.NoData || l < out {
			out = l
		}
	}
	return out
}

func (e *Evaluator) query(ctx context.Context, src, expr string, ts time.Time) ([]collector.Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	return e.q.Query(ctx, src, expr, ts)
}

func (e *Evaluator) evalSignal(ctx context.Context, s *catalog.Signal, ts time.Time) SignalResult {
	sr := SignalResult{Name: s.Name, Kind: s.Kind, Level: model.NoData}
	switch s.Kind {
	case catalog.KindProbeVote:
		res, err := e.query(ctx, s.Source, s.ProbeQuery(), ts)
		if err != nil {
			sr.Error = err.Error()
			return sr
		}
		votes := map[string]float64{}
		for _, smp := range res {
			if !math.IsNaN(smp.Value) {
				votes[smp.Labels["probe_region"]] = smp.Value
			}
		}
		sr.Level, sr.Detail = Vote(votes, s.FailBelow, s.MinRegions)
	case catalog.KindPromQL:
		res, err := e.query(ctx, s.Source, s.Expr, ts)
		if err != nil {
			sr.Error = err.Error()
			return sr
		}
		sr.Level, sr.Value, sr.Detail = Threshold(res, s)
	case catalog.KindSerialAge:
		res, err := e.query(ctx, s.Source, s.Expr, ts)
		if err != nil {
			sr.Error = err.Error()
			return sr
		}
		age, detail, ok := SerialAge(res, s.SerialFormat, s.Aggregate, ts)
		if !ok {
			sr.Detail = detail
			return sr
		}
		sr.Value = &age
		sr.Level = AgeLevel(age, s.DegradedAfter.Duration, s.DownAfter.Duration)
		sr.Detail = detail + ageLimits(s.DegradedAfter.Duration, s.DownAfter.Duration)
	}
	if capped := s.Cap(sr.Level); capped != sr.Level {
		sr.Level = capped
		sr.Detail += " [capped at degraded]"
	}
	return sr
}

// SerialAge converts SOA serials to ages at ts and aggregates them. ok is false
// when no series holds a valid serial.
func SerialAge(res []collector.Sample, format, agg string, ts time.Time) (age float64, detail string, ok bool) {
	var ages []float64
	bad := 0
	for _, smp := range res {
		t, valid := catalog.SerialTime(smp.Value, format)
		if !valid {
			bad++
			continue
		}
		a := ts.Sub(t).Seconds()
		if a < 0 {
			a = 0
		}
		ages = append(ages, a)
	}
	if len(ages) == 0 {
		if bad > 0 {
			return 0, fmt.Sprintf("%d series, none with a valid %s serial", bad, format), false
		}
		return 0, "no series", false
	}
	sort.Float64s(ages)
	age = pick(ages, agg)
	detail = fmt.Sprintf("%s serial age %s over %d series (newest %s, oldest %s)", agg, fmtAge(age), len(ages), fmtAge(ages[0]), fmtAge(ages[len(ages)-1]))
	return age, detail, true
}

// pick aggregates sorted ages: oldest (default), median or freshest.
func pick(sorted []float64, agg string) float64 {
	n := len(sorted)
	switch agg {
	case catalog.AggFreshest:
		return sorted[0]
	case catalog.AggMedian:
		if n%2 == 1 {
			return sorted[n/2]
		}
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n-1]
}

// AgeLevel maps an age in seconds to a level.
func AgeLevel(age float64, degradedAfter, downAfter time.Duration) model.Level {
	switch {
	case downAfter > 0 && age > downAfter.Seconds():
		return model.Down
	case age > degradedAfter.Seconds():
		return model.Degraded
	}
	return model.OK
}

func ageLimits(degradedAfter, downAfter time.Duration) string {
	s := fmt.Sprintf(" (degraded after %s", degradedAfter)
	if downAfter > 0 {
		s += ", down after " + downAfter.String()
	}
	return s + ")"
}

func fmtAge(sec float64) string {
	return time.Duration(sec * float64(time.Second)).Round(time.Second).String()
}

// Vote: 0 failing regions = OK, 1 = degraded, >=2 = down; fewer than
// minRegions reporting = NoData (a silent region is not an outage).
func Vote(regionSuccess map[string]float64, failBelow float64, minRegions int) (model.Level, string) {
	regions := make([]string, 0, len(regionSuccess))
	for r := range regionSuccess {
		regions = append(regions, r)
	}
	sort.Strings(regions)
	failing := 0
	parts := make([]string, 0, len(regions))
	for _, r := range regions {
		v := regionSuccess[r]
		mark := "ok"
		if v < failBelow {
			failing++
			mark = "FAIL"
		}
		parts = append(parts, fmt.Sprintf("%s=%.2f(%s)", r, v, mark))
	}
	detail := fmt.Sprintf("%d/%d regions failing: %s", failing, len(regions), strings.Join(parts, " "))
	if len(regions) < minRegions {
		return model.NoData, fmt.Sprintf("only %d region(s) reporting (need %d) %s", len(regions), minRegions, strings.Join(parts, " "))
	}
	switch {
	case failing == 0:
		return model.OK, detail
	case failing == 1:
		return model.Degraded, detail
	default:
		return model.Down, detail
	}
}

// Threshold evaluates a promql signal. With several series the worst wins.
func Threshold(res []collector.Sample, s *catalog.Signal) (model.Level, *float64, string) {
	if len(res) == 0 {
		switch s.Empty {
		case "ok":
			return model.OK, nil, "no series (treated as ok)"
		case "degraded":
			return model.Degraded, nil, "no series (treated as degraded)"
		case "down":
			return model.Down, nil, "no series (treated as down)"
		}
		return model.NoData, nil, "no series"
	}
	worst := model.NoData
	var worstVal *float64
	for _, smp := range res {
		v := smp.Value
		if math.IsNaN(v) {
			continue
		}
		l := model.Down
		switch {
		case s.OK.Match(v):
			l = model.OK
		case s.Degraded.IsSet() && s.Degraded.Match(v):
			l = model.Degraded
		}
		if worst == model.NoData || l < worst {
			worst = l
			vv := v
			worstVal = &vv
		}
	}
	if worst == model.NoData {
		return model.NoData, nil, "NaN result"
	}
	detail := fmt.Sprintf("value %s (ok %s", fmtFloat(*worstVal), s.OK)
	if s.Degraded.IsSet() {
		detail += ", degraded " + s.Degraded.String()
	}
	detail += ")"
	if len(res) > 1 {
		detail += fmt.Sprintf(", worst of %d series", len(res))
	}
	return worst, worstVal, detail
}

func (e *Evaluator) evalFreshness(ctx context.Context, f *catalog.Freshness, ts time.Time) (*FreshnessResult, SignalResult) {
	sr := SignalResult{Name: "freshness", Kind: "freshness", Level: model.NoData}
	res, err := e.query(ctx, f.Source, f.Expr, ts)
	if err != nil {
		sr.Error = err.Error()
		return nil, sr
	}
	var age float64
	if f.SerialFormat != "" {
		a, detail, ok := SerialAge(res, f.SerialFormat, f.Aggregate, ts)
		if !ok {
			sr.Detail = detail
			return nil, sr
		}
		age, sr.Detail = a, detail
	} else {
		var ages []float64
		for _, smp := range res {
			if !math.IsNaN(smp.Value) {
				ages = append(ages, math.Max(smp.Value, 0))
			}
		}
		if len(ages) == 0 {
			sr.Detail = "no series"
			return nil, sr
		}
		sort.Float64s(ages)
		age = pick(ages, f.Aggregate)
		sr.Detail = "age " + fmtAge(age)
	}
	l := AgeLevel(age, f.DegradedAfter.Duration, f.DownAfter.Duration)
	sr.Level = l
	sr.Value = &age
	sr.Detail += ageLimits(f.DegradedAfter.Duration, f.DownAfter.Duration)
	return &FreshnessResult{AgeSeconds: age, Level: l}, sr
}

func fmtFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e12 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.4g", v)
}

// Hysteresis filters raw levels: the published level gets worse only after
// downAfter consecutive worse evaluations, and better only after upAfter
// consecutive better ones. When the streak mixes levels, the least extreme
// one is published (e.g. degraded,down,degraded from OK publishes degraded).
// NoData evaluations neither count nor break a streak.
type Hysteresis struct {
	pub    model.Level
	init   bool
	recent []model.Level
}

func (h *Hysteresis) Published() model.Level { return h.pub }

func (h *Hysteresis) Step(raw model.Level, downAfter, upAfter int) model.Level {
	if !h.init {
		// No history: assume OK so a bad first reading still needs downAfter
		// confirmations before it is published.
		h.pub, h.init = model.OK, true
	}
	if raw == model.NoData {
		return h.pub
	}
	h.recent = append(h.recent, raw)
	if max := maxInt(downAfter, upAfter); len(h.recent) > max {
		h.recent = h.recent[len(h.recent)-max:]
	}
	if raw == h.pub {
		return h.pub
	}
	n := upAfter
	if raw < h.pub {
		n = downAfter
	}
	if len(h.recent) < n {
		return h.pub
	}
	tail := h.recent[len(h.recent)-n:]
	if raw < h.pub {
		best := model.Down
		for _, l := range tail {
			if l >= h.pub {
				return h.pub
			}
			if l > best {
				best = l
			}
		}
		h.pub = best
	} else {
		worst := model.OK
		for _, l := range tail {
			if l <= h.pub {
				return h.pub
			}
			if l < worst {
				worst = l
			}
		}
		h.pub = worst
	}
	return h.pub
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
