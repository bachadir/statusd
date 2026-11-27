package catalog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"statusd/internal/model"
)

// The example catalogue must always load: CI guard for catalog/*.yml edits.
func TestShippedCatalogLoads(t *testing.T) {
	cat, err := Load("../../catalog", map[string]bool{"prom": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Groups) == 0 {
		t.Fatal("the example catalogue has no groups")
	}
	for _, c := range cat.Components() {
		for _, s := range c.Signals {
			if s.Kind == KindProbeVote && s.MinRegions < 2 {
				t.Errorf("%s: probe votes need >= 2 regions", c.ID)
			}
		}
	}
}

func TestValidation(t *testing.T) {
	bad := map[string]string{
		"unknown kind":         "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: nope, source: s}]}\n",
		"unknown source":       "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: promql, source: zz, expr: up, ok: '>0'}]}\n",
		"bad comparator":       "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: promql, source: s, expr: up, ok: 'big'}]}\n",
		"bad slug":             "group: {slug: G!, name: G}\ncomponents:\n  - {slug: a, name: A}\n",
		"dup component":        "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A}\n  - {slug: a, name: B}\n",
		"unknown field":        "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, colour: red}\n",
		"quote in check":       "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{kind: probe_vote, source: s, check: 'a\"b'}]}\n",
		"serial_age no format": "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: serial_age, source: s, expr: up, degraded_after: 15m}]}\n",
		"serial_age no limit":  "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: serial_age, source: s, expr: up, serial_format: unix}]}\n",
		"serial_age bad agg":   "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: serial_age, source: s, expr: up, serial_format: unix, aggregate: mean, degraded_after: 1m}]}\n",
		"bad max_level":        "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: promql, source: s, expr: up, ok: '>0', max_level: ok}]}\n",
		"freshness bad format": "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, freshness: {source: s, expr: up, serial_format: yyyy, degraded_after: 1m}}\n",
		"statusio bad id":      "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, statusio: {component: abc, container: fedcba9876543210fedcba98}}\n",
		"statusio dup pair": "group: {slug: g, name: G}\ncomponents:\n" +
			"  - {slug: a, name: A, statusio: {component: fedcba9876543210fedcba98, container: 0123456789abcdef01234567}}\n" +
			"  - {slug: b, name: B, statusio: {component: fedcba9876543210fedcba98, container: 0123456789abcdef01234567}}\n",
	}
	for name, y := range bad {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "x.yml"), []byte(y), 0o644)
		if _, err := Load(dir, map[string]bool{"s": true}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestComparator(t *testing.T) {
	for _, c := range []struct {
		s    string
		v    float64
		want bool
	}{{">=0.9", 0.9, true}, {">=0.9", 0.89, false}, {"<0.05", 0.01, true}, {"==1", 1, true}, {"!= 0", 0, false}, {"> -1", 0, true}} {
		cmp, err := ParseComparator(c.s)
		if err != nil {
			t.Fatal(err)
		}
		if cmp.Match(c.v) != c.want {
			t.Errorf("%s %v", c.s, c.v)
		}
	}
}

// A container may repeat under different Status.io components (they are shared).
func TestStatusioSharedContainer(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "x.yml"), []byte("group: {slug: g, name: G}\ncomponents:\n"+
		"  - {slug: a, name: A, statusio: {component: fedcba9876543210fedcba98, container: 0123456789abcdef01234567}}\n"+
		"  - {slug: b, name: B, statusio: {component: abcdefabcdefabcdefabcdef, container: 0123456789abcdef01234567}}\n"), 0o644)
	if _, err := Load(dir, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSerialTime(t *testing.T) {
	cases := []struct {
		serial float64
		format string
		want   string // RFC3339, "" = invalid
	}{
		{2611201036, SerialYYMMDDHHMM, "2026-11-20T10:36:00Z"},
		{2612312359, SerialYYMMDDHHMM, "2026-12-31T23:59:00Z"},
		{2613011200, SerialYYMMDDHHMM, ""}, // month 13
		{2602301200, SerialYYMMDDHHMM, ""}, // Feb 30
		{2611201060, SerialYYMMDDHHMM, ""}, // minute 60
		{1795170841, SerialUnix, "2026-11-20T10:34:01Z"},
		{0, SerialUnix, ""},
	}
	for _, c := range cases {
		got, ok := SerialTime(c.serial, c.format)
		if c.want == "" {
			if ok {
				t.Errorf("%v %s: want invalid, got %v", c.serial, c.format, got)
			}
			continue
		}
		if !ok || got.Format(time.RFC3339) != c.want {
			t.Errorf("%v %s: got %v (%v), want %s", c.serial, c.format, got, ok, c.want)
		}
	}
}

func TestSerialAgeDefaults(t *testing.T) {
	dir := t.TempDir()
	y := "group: {slug: g, name: G}\ncomponents:\n  - {slug: a, name: A, signals: [{name: x, kind: serial_age, source: s, expr: up, serial_format: unix, degraded_after: 15m, max_level: degraded}]}\n"
	if err := os.WriteFile(filepath.Join(dir, "g.yml"), []byte(y), 0o644); err != nil {
		t.Fatal(err)
	}
	cat, err := Load(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := cat.Component("g/a").Signals[0]
	if s.Aggregate != AggOldest {
		t.Errorf("aggregate default: got %q", s.Aggregate)
	}
	if s.Cap(model.Down) != model.Degraded || s.Cap(model.OK) != model.OK {
		t.Errorf("max_level degraded must cap down only")
	}
}
