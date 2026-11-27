package eval

import (
	"context"
	"math"
	"testing"
	"time"

	"statusd/internal/catalog"
	"statusd/internal/collector"
	"statusd/internal/model"
)

const (
	N = model.NoData
	D = model.Down
	G = model.Degraded
	K = model.OK
)

func TestHysteresis(t *testing.T) {
	cases := []struct {
		name     string
		in, want []model.Level
	}{
		{"single blip ignored", []model.Level{D, K, K, K}, []model.Level{K, K, K, K}},
		{"down after 3", []model.Level{D, D, D, D}, []model.Level{K, K, D, D}},
		{"mixed bad streak publishes least severe", []model.Level{G, D, G}, []model.Level{K, K, G}},
		{"recover needs 5", []model.Level{D, D, D, K, K, K, K, K}, []model.Level{K, K, D, D, D, D, D, K}},
		{"recovery interrupted", []model.Level{D, D, D, K, K, K, K, D, K}, []model.Level{K, K, D, D, D, D, D, D, D}},
		{"no data neither counts nor breaks", []model.Level{D, N, D, N, D}, []model.Level{K, K, K, K, D}},
		{"no data keeps last", []model.Level{N, N, N}, []model.Level{K, K, K}},
		{"degraded to down", []model.Level{G, G, G, D, D, D}, []model.Level{K, K, G, G, G, D}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var h Hysteresis
			for i, raw := range c.in {
				if got := h.Step(raw, 3, 5); got != c.want[i] {
					t.Fatalf("step %d (raw %v): got %v want %v (all: %v)", i, raw, got, c.want[i], c.want)
				}
			}
		})
	}
}

func TestVote(t *testing.T) {
	cases := []struct {
		name  string
		votes map[string]float64
		want  model.Level
	}{
		{"all ok", map[string]float64{"eu": 1, "us": 1, "apac": 0.9}, K},
		{"one region failing", map[string]float64{"eu": 0.2, "us": 1, "apac": 1}, G},
		{"two failing", map[string]float64{"eu": 0.2, "us": 0.1, "apac": 1}, D},
		{"all failing", map[string]float64{"eu": 0, "us": 0, "apac": 0}, D},
		{"one silent region", map[string]float64{"eu": 1, "us": 1}, K},
		{"two silent", map[string]float64{"eu": 0}, N},
		{"nothing", map[string]float64{}, N},
		{"two report, one fails", map[string]float64{"eu": 0, "us": 1}, G},
		{"exactly at threshold is ok", map[string]float64{"eu": 0.5, "us": 1, "apac": 1}, K},
	}
	for _, c := range cases {
		if got, _ := Vote(c.votes, 0.5, 2); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func cmp(s string) catalog.Comparator { c, _ := catalog.ParseComparator(s); return c }

func TestThreshold(t *testing.T) {
	s := &catalog.Signal{OK: cmp("<0.05"), Degraded: cmp("<0.25")}
	smp := func(v ...float64) []collector.Sample {
		var out []collector.Sample
		for _, x := range v {
			out = append(out, collector.Sample{Value: x})
		}
		return out
	}
	if l, _, _ := Threshold(smp(0.01), s); l != K {
		t.Errorf("ok: %v", l)
	}
	if l, _, _ := Threshold(smp(0.1), s); l != G {
		t.Errorf("degraded: %v", l)
	}
	if l, _, _ := Threshold(smp(0.5), s); l != D {
		t.Errorf("down: %v", l)
	}
	if l, _, _ := Threshold(smp(0.01, 0.5), s); l != D {
		t.Errorf("worst series wins: %v", l)
	}
	if l, _, _ := Threshold(smp(math.NaN()), s); l != N {
		t.Errorf("NaN is no data: %v", l)
	}
	if l, _, _ := Threshold(nil, s); l != N {
		t.Errorf("empty default no data: %v", l)
	}
	s.Empty = "ok"
	if l, _, _ := Threshold(nil, s); l != K {
		t.Errorf("empty=ok: %v", l)
	}
}

func TestCombine(t *testing.T) {
	if Combine([]model.Level{K, N, G}) != G {
		t.Error("worst known wins")
	}
	if Combine([]model.Level{N, N}) != N {
		t.Error("all no data")
	}
	if Combine(nil) != N {
		t.Error("nothing")
	}
}

// fakeQ answers the probe query with per-region values and errors otherwise.
type fakeQ struct{ regions map[string]float64 }

func (f fakeQ) Query(ctx context.Context, src, expr string, ts time.Time) ([]collector.Sample, error) {
	var out []collector.Sample
	for r, v := range f.regions {
		out = append(out, collector.Sample{Labels: map[string]string{"probe_region": r}, Value: v})
	}
	return out, nil
}

func TestEvaluateEndToEnd(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir+"/x.yml", `
group: {slug: web, name: "Web", order: 1}
components:
  - slug: site
    name: "Site"
    hysteresis: {down_after: 2, up_after: 2}
    degraded_as: partial_outage
    signals:
      - {kind: probe_vote, source: prom, check: 'www_.*'}
  - slug: manual
    name: "Manual thing"
`)
	cat, err := catalog.Load(dir, map[string]bool{"prom": true})
	if err != nil {
		t.Fatal(err)
	}
	q := fakeQ{regions: map[string]float64{"eu": 0, "us": 1, "apac": 1}}
	e := New(cat, q)
	r1 := e.Evaluate(context.Background(), time.Now())
	if r1[0].Raw != G || r1[0].Status != model.Operational {
		t.Fatalf("first: %+v", r1[0])
	}
	r2 := e.Evaluate(context.Background(), time.Now())
	if r2[0].Status != model.PartialOutage {
		t.Fatalf("second should publish degraded as partial_outage: %+v", r2[0])
	}
	if r2[1].Automated || r2[1].Status != model.Operational {
		t.Fatalf("manual component: %+v", r2[1])
	}
}

func TestDisplayOnlyFreshness(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir+"/x.yml", `
group: {slug: feeds, name: "Feeds", order: 1}
components:
  - slug: ip
    name: "Daily export"
    freshness: {source: prom, expr: 'age', degraded_after: 1h, down_after: 4h, affects_status: false, public: false}
`)
	cat, err := catalog.Load(dir, map[string]bool{"prom": true})
	if err != nil {
		t.Fatal(err)
	}
	// Very old data: the freshness level is down, but it must not touch the status.
	r := New(cat, fakeQ{regions: map[string]float64{"x": 99999}}).Evaluate(context.Background(), time.Now())[0]
	if r.Automated || r.Status != model.Operational {
		t.Fatalf("display-only freshness changed the status: %+v", r)
	}
	if len(r.Signals) != 1 || r.Signals[0].Level != D || r.Signals[0].Kind != "freshness (display only)" {
		t.Fatalf("display-only freshness should be listed for staff: %+v", r.Signals)
	}
	if r.Freshness == nil || r.Freshness.Level != D {
		t.Fatalf("freshness result: %+v", r.Freshness)
	}
}

func TestSerialAge(t *testing.T) {
	ts := time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	res := []collector.Sample{{Value: 2611200555}, {Value: 2611200500}, {Value: 2611200300}, {Value: 1}} // 5m, 60m, 180m, invalid
	cases := map[string]float64{catalog.AggOldest: 180 * 60, catalog.AggMedian: 60 * 60, catalog.AggFreshest: 5 * 60}
	for agg, want := range cases {
		got, _, ok := SerialAge(res, catalog.SerialYYMMDDHHMM, agg, ts)
		if !ok || got != want {
			t.Errorf("%s: got %v (%v), want %v", agg, got, ok, want)
		}
	}
	if _, _, ok := SerialAge([]collector.Sample{{Value: 1}}, catalog.SerialYYMMDDHHMM, catalog.AggOldest, ts); ok {
		t.Error("only invalid serials must be no data")
	}
	if AgeLevel(16*60, 15*time.Minute, time.Hour) != G || AgeLevel(61*60, 15*time.Minute, time.Hour) != D || AgeLevel(60, 15*time.Minute, 0) != K {
		t.Error("AgeLevel thresholds")
	}
}

type fixedQ struct{ res []collector.Sample }

func (f fixedQ) Query(context.Context, string, string, time.Time) ([]collector.Sample, error) {
	return f.res, nil
}

func TestSerialAgeSignalCapped(t *testing.T) {
	s := &catalog.Signal{Name: "zone-age", Kind: catalog.KindSerialAge, Source: "s", Expr: "x",
		SerialFormat: catalog.SerialYYMMDDHHMM, Aggregate: catalog.AggOldest, MaxLevel: "degraded",
		DegradedAfter: catalog.Duration{Duration: 15 * time.Minute}, DownAfter: catalog.Duration{Duration: time.Hour}}
	ts := time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	e := &Evaluator{q: fixedQ{[]collector.Sample{{Value: 2611200300}}}, timeout: time.Second}
	sr := e.evalSignal(context.Background(), s, ts)
	if sr.Level != G {
		t.Fatalf("3h old serial with max_level degraded: got %v (%s)", sr.Level, sr.Detail)
	}
	s.MaxLevel = ""
	if sr := e.evalSignal(context.Background(), s, ts); sr.Level != D {
		t.Fatalf("uncapped: got %v", sr.Level)
	}
}
