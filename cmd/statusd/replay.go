package main

// Replay: run the catalogue against past Prometheus data and print what the
// status page would have published. For tuning signals before they ship:
//
//	statusd -config statusd.yaml -replay-from 2026-11-13T00:00:00Z [-replay-to …]
//	        [-replay-match '^(api|web)/'] [-replay-catalog ./catalog] [-replay-step 60s]
//
// Each distinct query is fetched once as a range query (in chunks), and the
// evaluator's instant queries are answered from that cache: a range query is the
// instant query evaluated at every step, so the levels are the same as live,
// including hysteresis.

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/collector"
	"statusd/internal/eval"
	"statusd/internal/model"
)

type replayOpts struct {
	from, to time.Time
	step     time.Duration
	match    string
	catalog  string
	signals  bool
}

// rangeCache answers instant queries from range queries over the replay window.
type rangeCache struct {
	col              *collector.Collector
	start, end       time.Time
	step             time.Duration
	mu               sync.Mutex
	entries          map[string]*cacheEntry
	queries, samples int
}

type cacheEntry struct {
	once   sync.Once
	series []collector.Series
	err    error
}

const maxPointsPerQuery = 10000 // Prometheus refuses more than 11000 points per series

func (c *rangeCache) Query(ctx context.Context, src, expr string, ts time.Time) ([]collector.Sample, error) {
	key := src + "\x00" + expr
	c.mu.Lock()
	e := c.entries[key]
	if e == nil {
		e = &cacheEntry{}
		c.entries[key] = e
	}
	c.mu.Unlock()
	e.once.Do(func() { e.series, e.err = c.fetch(src, expr) })
	if e.err != nil {
		return nil, e.err
	}
	t := ts.Unix()
	var out []collector.Sample
	for _, s := range e.series {
		if v, ok := s.Points[t]; ok {
			out = append(out, collector.Sample{Labels: s.Labels, Value: v})
		}
	}
	return out, nil
}

func (c *rangeCache) fetch(src, expr string) ([]collector.Series, error) {
	byKey := map[string]*collector.Series{}
	chunk := time.Duration(maxPointsPerQuery-1) * c.step
	for a := c.start; !a.After(c.end); a = a.Add(chunk + c.step) {
		b := a.Add(chunk)
		if b.After(c.end) {
			b = c.end
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		res, err := c.col.QueryRange(ctx, src, expr, a, b, c.step)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", src, err)
		}
		c.mu.Lock()
		c.queries++
		c.mu.Unlock()
		for _, s := range res {
			k := labelKey(s.Labels)
			if byKey[k] == nil {
				byKey[k] = &collector.Series{Labels: s.Labels, Points: map[int64]float64{}}
			}
			for t, v := range s.Points {
				byKey[k].Points[t] = v
			}
		}
	}
	out := make([]collector.Series, 0, len(byKey))
	for _, s := range byKey {
		out = append(out, *s)
	}
	return out, nil
}

func labelKey(l map[string]string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "=" + l[k] + ",")
	}
	return b.String()
}

type episode struct {
	component  string
	start, end time.Time
	worst      model.Status
	signals    map[string]bool // signals that were not OK while the episode was published
	open       bool
}

func runReplay(cfgPath string, o replayOpts, w io.Writer) error {
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}
	col, err := collector.New(cfg.Sources)
	if err != nil {
		return err
	}
	dir := cfg.CatalogDir
	if o.catalog != "" {
		dir = o.catalog
	}
	cat, err := catalog.Load(dir, col.Sources())
	if err != nil {
		return err
	}
	if o.match != "" {
		re, err := regexp.Compile(o.match)
		if err != nil {
			return fmt.Errorf("replay-match: %w", err)
		}
		for _, g := range cat.Groups {
			var keep []*catalog.Component
			for _, c := range g.Components {
				if re.MatchString(c.ID) {
					keep = append(keep, c)
				}
			}
			g.Components = keep
		}
	}
	if o.step <= 0 {
		o.step = cfg.Evaluation.Interval
	}
	if o.step <= 0 {
		o.step = time.Minute
	}
	start := o.from.Truncate(o.step)
	end := o.to.Truncate(o.step)
	if !end.After(start) {
		return fmt.Errorf("replay-to must be after replay-from")
	}
	cache := &rangeCache{col: col, start: start, end: end, step: o.step, entries: map[string]*cacheEntry{}}
	ev := eval.New(cat, cache)

	status := map[string]model.Status{}
	var episodes []*episode
	openEp := map[string]*episode{}
	steps := 0
	for ts := start; !ts.After(end); ts = ts.Add(o.step) {
		steps++
		for _, r := range ev.Evaluate(context.Background(), ts) {
			prev, seen := status[r.ID]
			if !seen {
				prev = model.Operational
			}
			status[r.ID] = r.Status
			ep := openEp[r.ID]
			if r.Status != model.Operational {
				if ep == nil {
					ep = &episode{component: r.ID, start: ts, worst: r.Status, signals: map[string]bool{}, open: true}
					openEp[r.ID] = ep
					episodes = append(episodes, ep)
				}
				ep.end = ts
				if r.Status.Severity() > ep.worst.Severity() {
					ep.worst = r.Status
				}
				for _, s := range r.Signals {
					if s.Level == model.Down || s.Level == model.Degraded {
						ep.signals[s.Name] = true
					}
				}
			} else if ep != nil && prev != model.Operational {
				ep.end, ep.open = ts, false
				delete(openEp, r.ID)
			}
		}
	}

	fmt.Fprintf(w, "replay %s -> %s UTC, step %s, %d evaluations, %d range queries\n",
		start.UTC().Format("2006-01-02 15:04"), end.UTC().Format("2006-01-02 15:04"), o.step, steps, cache.queries)
	fmt.Fprintf(w, "published episodes (status other than operational):\n")
	sort.SliceStable(episodes, func(i, j int) bool {
		if episodes[i].component != episodes[j].component {
			return episodes[i].component < episodes[j].component
		}
		return episodes[i].start.Before(episodes[j].start)
	})
	total := map[string]time.Duration{}
	for _, e := range episodes {
		d := e.end.Sub(e.start)
		total[e.component] += d
		suffix := ""
		if e.open {
			suffix = " (still open)"
		}
		line := fmt.Sprintf("  %-28s %s -> %s %5.0f min  %-22s%s", e.component,
			e.start.UTC().Format("01-02 15:04"), e.end.UTC().Format("01-02 15:04"), d.Minutes(), e.worst, suffix)
		if o.signals {
			var names []string
			for n := range e.signals {
				names = append(names, n)
			}
			sort.Strings(names)
			line += "  [" + strings.Join(names, ",") + "]"
		}
		fmt.Fprintln(w, line)
	}
	if len(episodes) == 0 {
		fmt.Fprintln(w, "  none")
	}
	return nil
}
