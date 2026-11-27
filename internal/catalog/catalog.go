// Package catalog loads the service catalogue (catalog/*.yml): public groups and
// components, and the signals that drive each component's automatic status.
package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"statusd/internal/model"
)

// Duration is a time.Duration that unmarshals from "5m"-style strings.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	d.Duration = v
	return nil
}

// Comparator is a threshold such as ">=0.9" or "<0.05".
type Comparator struct {
	Op    string
	Value float64
	raw   string
}

var cmpRe = regexp.MustCompile(`^\s*(<=|>=|==|!=|<|>)\s*(-?[0-9.eE+-]+)\s*$`)

func ParseComparator(s string) (Comparator, error) {
	m := cmpRe.FindStringSubmatch(s)
	if m == nil {
		return Comparator{}, fmt.Errorf("invalid comparator %q (want e.g. \">=0.9\")", s)
	}
	v, err := strconv.ParseFloat(m[2], 64)
	if err != nil {
		return Comparator{}, fmt.Errorf("invalid comparator value in %q: %w", s, err)
	}
	return Comparator{Op: m[1], Value: v, raw: s}, nil
}

func (c *Comparator) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	p, err := ParseComparator(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*c = p
	return nil
}

func (c Comparator) IsSet() bool    { return c.Op != "" }
func (c Comparator) String() string { return c.raw }

func (c Comparator) Match(v float64) bool {
	switch c.Op {
	case "<":
		return v < c.Value
	case "<=":
		return v <= c.Value
	case ">":
		return v > c.Value
	case ">=":
		return v >= c.Value
	case "==":
		return v == c.Value
	case "!=":
		return v != c.Value
	}
	return false
}

// Signal kinds.
const (
	KindProbeVote = "probe_vote" // blackbox probe_success voted across probe regions
	KindPromQL    = "promql"     // arbitrary instant query compared against thresholds
	KindSerialAge = "serial_age" // age of DNS SOA serials (e.g. probe_dns_serial from a DNS prober)
)

// Serial formats for serial_age signals and freshness.
const (
	SerialYYMMDDHHMM = "yymmddhhmm" // 2611201036 = 2026-11-20 10:36 UTC (a common SOA serial scheme)
	SerialUnix       = "unix"       // seconds since 1970
)

// Aggregations over several series' ages.
const (
	AggOldest   = "oldest"   // the worst series: any stale server counts
	AggMedian   = "median"   // what a typical client gets
	AggFreshest = "freshest" // the best series: stale only when every server is
)

// Signal is one input to a component's automatic status.
type Signal struct {
	Name   string `yaml:"name"`
	Kind   string `yaml:"kind"`
	Source string `yaml:"source"`

	// probe_vote
	Check      string   `yaml:"check"`       // regex on the probe "check" label
	Window     Duration `yaml:"window"`      // default 10m, a common dashboard window
	FailBelow  float64  `yaml:"fail_below"`  // region fails when success ratio < this (default 0.5)
	MinRegions int      `yaml:"min_regions"` // fewer reporting regions => no_data (default 2)

	// promql
	Expr     string     `yaml:"expr"`
	OK       Comparator `yaml:"ok"`
	Degraded Comparator `yaml:"degraded"`
	// Empty decides the level when the query returns no series: "no_data"
	// (default), "ok", "degraded" or "down".
	Empty string `yaml:"empty"`

	// serial_age: Expr returns SOA serials (one series per server); their ages
	// are aggregated and compared with DegradedAfter/DownAfter.
	SerialFormat  string   `yaml:"serial_format"`
	Aggregate     string   `yaml:"aggregate"`
	DegradedAfter Duration `yaml:"degraded_after"`
	DownAfter     Duration `yaml:"down_after"`

	// MaxLevel caps how bad this signal can make the component: "degraded"
	// makes it a softer, early-warning signal that never reports down alone.
	MaxLevel string `yaml:"max_level"`

	Link string `yaml:"link"` // optional Grafana link shown in the control panel

	checkRe *regexp.Regexp
}

// Cap returns l limited by MaxLevel.
func (s *Signal) Cap(l model.Level) model.Level {
	if s.MaxLevel == "degraded" && l == model.Down {
		return model.Degraded
	}
	return l
}

// Freshness describes "how old is the data customers get" for a component.
// Expr returns an age in seconds per series, or with SerialFormat set, an SOA
// serial per series. Aggregate picks the series (default oldest: the worst wins).
type Freshness struct {
	Source        string   `yaml:"source"`
	Expr          string   `yaml:"expr"`
	SerialFormat  string   `yaml:"serial_format"`
	Aggregate     string   `yaml:"aggregate"`
	DegradedAfter Duration `yaml:"degraded_after"`
	DownAfter     Duration `yaml:"down_after"`
	// AffectsStatus: when true (default) a stale dataset also degrades the
	// component's automatic status; when false it is display-only.
	AffectsStatus *bool `yaml:"affects_status"`
	Public        *bool `yaml:"public"` // show the age on the public page (default true)
}

func (f *Freshness) Affects() bool  { return f.AffectsStatus == nil || *f.AffectsStatus }
func (f *Freshness) IsPublic() bool { return f.Public == nil || *f.Public }

type Hysteresis struct {
	DownAfter int `yaml:"down_after"` // consecutive bad evaluations before publishing worse
	UpAfter   int `yaml:"up_after"`   // consecutive good evaluations before publishing better
}

type Component struct {
	Slug        string     `yaml:"slug"`
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	Note        string     `yaml:"note"` // internal note shown in the control panel only
	Signals     []*Signal  `yaml:"signals"`
	Freshness   *Freshness `yaml:"freshness"`
	Hysteresis  Hysteresis `yaml:"hysteresis"`
	DegradedAs  string     `yaml:"degraded_as"`
	DownAs      string     `yaml:"down_as"`
	Hidden      bool       `yaml:"hidden"`
	// StatusIO maps the component to a Status.io (component, container) pair
	// (optional Status.io feed). Unmapped components are not pushed.
	StatusIO *StatusIORef `yaml:"statusio"`

	ID    string `yaml:"-"` // "<group>/<slug>", unique across the catalogue
	Group *Group `yaml:"-"`
}

// Automated reports whether the component has anything that can change its
// automatic status (signals, or freshness that affects status).
func (c *Component) Automated() bool {
	return len(c.Signals) > 0 || (c.Freshness != nil && c.Freshness.Affects())
}

func (c *Component) Mapping() model.Mapping {
	return model.Mapping{DegradedAs: model.Status(c.DegradedAs), DownAs: model.Status(c.DownAs)}
}

// StatusIORef is a Status.io status pair: component = our group, container =
// our component. Containers are shared between Status.io components, so both
// IDs are needed.
type StatusIORef struct {
	Component string `yaml:"component"`
	Container string `yaml:"container"`
}

var statusioIDRe = regexp.MustCompile(`^[0-9a-f]{24}$`)

type Group struct {
	Slug        string       `yaml:"slug"`
	Name        string       `yaml:"name"`
	Description string       `yaml:"description"`
	Order       int          `yaml:"order"`
	Collapsed   bool         `yaml:"collapsed"`
	Hidden      bool         `yaml:"hidden"`
	Components  []*Component `yaml:"-"`
}

type file struct {
	Group      Group        `yaml:"group"`
	Defaults   defaults     `yaml:"defaults"`
	Components []*Component `yaml:"components"`
}

type defaults struct {
	Hysteresis Hysteresis `yaml:"hysteresis"`
	DegradedAs string     `yaml:"degraded_as"`
	DownAs     string     `yaml:"down_as"`
}

type Catalog struct {
	Groups []*Group
	byID   map[string]*Component
}

func (c *Catalog) Component(id string) *Component { return c.byID[id] }

// Components returns all components in display order.
func (c *Catalog) Components() []*Component {
	var out []*Component
	for _, g := range c.Groups {
		out = append(out, g.Components...)
	}
	return out
}

var slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Load reads every *.yml / *.yaml file in dir. knownSources, when non-nil,
// restricts the "source" fields to configured Prometheus sources.
func Load(dir string, knownSources map[string]bool) (*Catalog, error) {
	var paths []string
	for _, pat := range []string{"*.yml", "*.yaml"} {
		m, _ := filepath.Glob(filepath.Join(dir, pat))
		paths = append(paths, m...)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("catalog: no *.yml files in %s", dir)
	}
	sort.Strings(paths)
	cat := &Catalog{byID: map[string]*Component{}}
	groups := map[string]string{}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		var f file
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true)
		if err := dec.Decode(&f); err != nil {
			return nil, fmt.Errorf("catalog %s: %w", filepath.Base(p), err)
		}
		if err := cat.add(&f, knownSources); err != nil {
			return nil, fmt.Errorf("catalog %s: %w", filepath.Base(p), err)
		}
		if prev, dup := groups[f.Group.Slug]; dup {
			return nil, fmt.Errorf("catalog: group %q defined in both %s and %s", f.Group.Slug, prev, filepath.Base(p))
		}
		groups[f.Group.Slug] = filepath.Base(p)
	}
	sort.SliceStable(cat.Groups, func(i, j int) bool { return cat.Groups[i].Order < cat.Groups[j].Order })
	pairs := map[StatusIORef]string{}
	for _, c := range cat.Components() {
		r := c.StatusIO
		if r == nil {
			continue
		}
		if !statusioIDRe.MatchString(r.Component) || !statusioIDRe.MatchString(r.Container) {
			return nil, fmt.Errorf("catalog: component %q: statusio component and container must be 24-character hex IDs", c.ID)
		}
		if prev, dup := pairs[*r]; dup {
			return nil, fmt.Errorf("catalog: components %q and %q map to the same status.io pair", prev, c.ID)
		}
		pairs[*r] = c.ID
	}
	return cat, nil
}

func (cat *Catalog) add(f *file, knownSources map[string]bool) error {
	g := f.Group
	if !slugRe.MatchString(g.Slug) {
		return fmt.Errorf("group slug %q must match %s", g.Slug, slugRe)
	}
	if strings.TrimSpace(g.Name) == "" {
		return fmt.Errorf("group %q has no name", g.Slug)
	}
	if len(f.Components) == 0 {
		return fmt.Errorf("group %q has no components", g.Slug)
	}
	gp := &g
	seen := map[string]bool{}
	for _, c := range f.Components {
		if !slugRe.MatchString(c.Slug) {
			return fmt.Errorf("component slug %q must match %s", c.Slug, slugRe)
		}
		if seen[c.Slug] {
			return fmt.Errorf("duplicate component %q", c.Slug)
		}
		seen[c.Slug] = true
		if strings.TrimSpace(c.Name) == "" {
			return fmt.Errorf("component %q has no name", c.Slug)
		}
		c.ID = g.Slug + "/" + c.Slug
		c.Group = gp
		if c.Hysteresis.DownAfter == 0 {
			c.Hysteresis.DownAfter = f.Defaults.Hysteresis.DownAfter
		}
		if c.Hysteresis.UpAfter == 0 {
			c.Hysteresis.UpAfter = f.Defaults.Hysteresis.UpAfter
		}
		if c.Hysteresis.DownAfter <= 0 {
			c.Hysteresis.DownAfter = 3
		}
		if c.Hysteresis.UpAfter <= 0 {
			c.Hysteresis.UpAfter = 5
		}
		if c.DegradedAs == "" {
			c.DegradedAs = f.Defaults.DegradedAs
		}
		if c.DownAs == "" {
			c.DownAs = f.Defaults.DownAs
		}
		for _, s := range []string{c.DegradedAs, c.DownAs} {
			if s != "" && !model.Status(s).Valid() {
				return fmt.Errorf("component %q: invalid status %q", c.Slug, s)
			}
		}
		names := map[string]bool{}
		for i, s := range c.Signals {
			if err := validateSignal(s, knownSources); err != nil {
				return fmt.Errorf("component %q signal #%d: %w", c.Slug, i+1, err)
			}
			if names[s.Name] {
				return fmt.Errorf("component %q: duplicate signal name %q", c.Slug, s.Name)
			}
			names[s.Name] = true
		}
		if fr := c.Freshness; fr != nil {
			if fr.Expr == "" || fr.Source == "" {
				return fmt.Errorf("component %q: freshness needs source and expr", c.Slug)
			}
			if knownSources != nil && !knownSources[fr.Source] {
				return fmt.Errorf("component %q: freshness: unknown source %q", c.Slug, fr.Source)
			}
			if fr.DegradedAfter.Duration <= 0 {
				return fmt.Errorf("component %q: freshness needs degraded_after", c.Slug)
			}
			if fr.DownAfter.Duration != 0 && fr.DownAfter.Duration <= fr.DegradedAfter.Duration {
				return fmt.Errorf("component %q: freshness down_after must be > degraded_after", c.Slug)
			}
			if err := validateSerial(fr.SerialFormat, &fr.Aggregate, false); err != nil {
				return fmt.Errorf("component %q: freshness: %w", c.Slug, err)
			}
		}
		if _, dup := cat.byID[c.ID]; dup {
			return fmt.Errorf("duplicate component id %q", c.ID)
		}
		cat.byID[c.ID] = c
		gp.Components = append(gp.Components, c)
	}
	cat.Groups = append(cat.Groups, gp)
	return nil
}

func validateSignal(s *Signal, knownSources map[string]bool) error {
	if s.Source == "" {
		return fmt.Errorf("missing source")
	}
	if knownSources != nil && !knownSources[s.Source] {
		return fmt.Errorf("unknown source %q", s.Source)
	}
	switch s.Kind {
	case KindProbeVote:
		if s.Check == "" {
			return fmt.Errorf("probe_vote needs check")
		}
		re, err := regexp.Compile("^(?:" + s.Check + ")$")
		if err != nil {
			return fmt.Errorf("check regex: %w", err)
		}
		s.checkRe = re
		if strings.ContainsAny(s.Check, `"\`) {
			return fmt.Errorf("check regex must not contain quotes or backslashes")
		}
		if s.Window.Duration == 0 {
			s.Window.Duration = 10 * time.Minute
		}
		if s.FailBelow == 0 {
			s.FailBelow = 0.5
		}
		if s.MinRegions == 0 {
			s.MinRegions = 2
		}
		if s.Name == "" {
			s.Name = "probes"
		}
	case KindPromQL:
		if s.Expr == "" {
			return fmt.Errorf("promql needs expr")
		}
		if !s.OK.IsSet() {
			return fmt.Errorf("promql needs ok threshold")
		}
		switch s.Empty {
		case "", "no_data", "ok", "degraded", "down":
		default:
			return fmt.Errorf("empty must be one of no_data, ok, degraded, down")
		}
		if s.Name == "" {
			return fmt.Errorf("promql signal needs a name")
		}
	case KindSerialAge:
		if s.Expr == "" {
			return fmt.Errorf("serial_age needs expr")
		}
		if s.Name == "" {
			return fmt.Errorf("serial_age signal needs a name")
		}
		if err := validateSerial(s.SerialFormat, &s.Aggregate, true); err != nil {
			return err
		}
		if s.DegradedAfter.Duration <= 0 {
			return fmt.Errorf("serial_age needs degraded_after")
		}
		if s.DownAfter.Duration != 0 && s.DownAfter.Duration <= s.DegradedAfter.Duration {
			return fmt.Errorf("serial_age down_after must be > degraded_after")
		}
	default:
		return fmt.Errorf("unknown kind %q", s.Kind)
	}
	switch s.MaxLevel {
	case "", "down", "degraded":
	default:
		return fmt.Errorf("max_level must be degraded or down")
	}
	return nil
}

// validateSerial checks serial_format and aggregate, defaulting aggregate to oldest.
func validateSerial(format string, agg *string, required bool) error {
	switch format {
	case SerialYYMMDDHHMM, SerialUnix:
	case "":
		if required {
			return fmt.Errorf("serial_format is required (%s or %s)", SerialYYMMDDHHMM, SerialUnix)
		}
	default:
		return fmt.Errorf("serial_format must be %s or %s", SerialYYMMDDHHMM, SerialUnix)
	}
	switch *agg {
	case "":
		*agg = AggOldest
	case AggOldest, AggMedian, AggFreshest:
	default:
		return fmt.Errorf("aggregate must be %s, %s or %s", AggOldest, AggMedian, AggFreshest)
	}
	return nil
}

// SerialTime converts an SOA serial to the time it encodes.
func SerialTime(serial float64, format string) (time.Time, bool) {
	if serial <= 0 || serial != serial {
		return time.Time{}, false
	}
	switch format {
	case SerialUnix:
		return time.Unix(int64(serial), 0).UTC(), true
	case SerialYYMMDDHHMM:
		v := int64(serial)
		yy, mo, dd, hh, mi := v/100000000, v/1000000%100, v/10000%100, v/100%100, v%100
		if v >= 10000000000 || mo < 1 || mo > 12 || dd < 1 || dd > 31 || hh > 23 || mi > 59 {
			return time.Time{}, false
		}
		t := time.Date(int(2000+yy), time.Month(mo), int(dd), int(hh), int(mi), 0, 0, time.UTC)
		if t.Day() != int(dd) { // e.g. Feb 30
			return time.Time{}, false
		}
		return t, true
	}
	return time.Time{}, false
}

// ProbeQuery is the PromQL used for a probe_vote signal: the worst check
// success ratio per probe region over the window.
func (s *Signal) ProbeQuery() string {
	return fmt.Sprintf(`min by (probe_region) (avg_over_time(probe_success{check=~"%s"}[%s]))`,
		s.Check, promDuration(s.Window.Duration))
}

func promDuration(d time.Duration) string {
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	return fmt.Sprintf("%ds", int(d/time.Second))
}
