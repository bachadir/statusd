// Package collector queries the Prometheus HTTP API of several estates. Each
// source has an ordered list of endpoints (direct Prometheus first, Grafana
// datasource proxy as a fallback); a failing endpoint is skipped for a while.
package collector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EndpointConfig struct {
	Name               string        `yaml:"name"`
	URL                string        `yaml:"url"` // base URL; /api/v1/query is appended
	BearerTokenFile    string        `yaml:"bearer_token_file"`
	BearerTokenEnv     string        `yaml:"bearer_token_env"`
	CAFile             string        `yaml:"ca_file"`
	InsecureSkipVerify bool          `yaml:"insecure_skip_verify"`
	Timeout            time.Duration `yaml:"timeout"`
}

type SourceConfig struct {
	Endpoints []EndpointConfig `yaml:"endpoints"`
}

type Sample struct {
	Labels map[string]string
	Value  float64
}

type endpoint struct {
	cfg       EndpointConfig
	client    *http.Client
	token     string
	downUntil time.Time
}

type source struct {
	name      string
	endpoints []*endpoint

	mu          sync.Mutex
	lastOK      time.Time
	lastErr     string
	lastErrAt   time.Time
	lastUsed    string
	okCount     uint64
	errCount    uint64
	consecutive int // consecutive failed queries (all endpoints)
}

// Health is a snapshot of one source's reachability.
type Health struct {
	Source      string    `json:"source"`
	Healthy     bool      `json:"healthy"`
	Endpoint    string    `json:"endpoint"`
	LastOK      time.Time `json:"last_ok"`
	LastError   string    `json:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at"`
	Queries     uint64    `json:"queries"`
	Errors      uint64    `json:"errors"`
}

type Collector struct {
	sources  map[string]*source
	cooldown time.Duration
	now      func() time.Time
}

func New(cfg map[string]SourceConfig) (*Collector, error) {
	c := &Collector{sources: map[string]*source{}, cooldown: time.Minute, now: time.Now}
	for name, sc := range cfg {
		if len(sc.Endpoints) == 0 {
			return nil, fmt.Errorf("source %q has no endpoints", name)
		}
		s := &source{name: name}
		for i, ec := range sc.Endpoints {
			ep, err := newEndpoint(ec)
			if err != nil {
				return nil, fmt.Errorf("source %q endpoint #%d: %w", name, i+1, err)
			}
			if ep.cfg.Name == "" {
				ep.cfg.Name = hostOf(ec.URL)
			}
			s.endpoints = append(s.endpoints, ep)
		}
		c.sources[name] = s
	}
	return c, nil
}

func hostOf(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return u
	}
	return p.Host
}

func newEndpoint(ec EndpointConfig) (*endpoint, error) {
	if ec.URL == "" {
		return nil, errors.New("missing url")
	}
	if ec.Timeout == 0 {
		ec.Timeout = 10 * time.Second
	}
	tlsCfg := &tls.Config{InsecureSkipVerify: ec.InsecureSkipVerify} //nolint:gosec // opt-in per endpoint
	if ec.CAFile != "" {
		pem, err := os.ReadFile(ec.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates in %s", ec.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	ep := &endpoint{cfg: ec, client: &http.Client{
		Timeout:   ec.Timeout,
		Transport: &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second},
	}}
	switch {
	case ec.BearerTokenFile != "":
		b, err := os.ReadFile(ec.BearerTokenFile)
		if err != nil {
			return nil, fmt.Errorf("bearer token: %w", err)
		}
		ep.token = strings.TrimSpace(string(b))
	case ec.BearerTokenEnv != "":
		ep.token = strings.TrimSpace(os.Getenv(ec.BearerTokenEnv))
		if ep.token == "" {
			return nil, fmt.Errorf("bearer token env %s is empty", ec.BearerTokenEnv)
		}
	}
	return ep, nil
}

// Sources lists configured source names.
func (c *Collector) Sources() map[string]bool {
	out := map[string]bool{}
	for n := range c.sources {
		out[n] = true
	}
	return out
}

// Query runs an instant query at ts against the named source.
func (c *Collector) Query(ctx context.Context, sourceName, expr string, ts time.Time) ([]Sample, error) {
	s, ok := c.sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("unknown source %q", sourceName)
	}
	now := c.now()
	var errs []string
	tried := 0
	for pass := 0; pass < 2 && tried == 0; pass++ {
		for _, ep := range s.endpoints {
			s.mu.Lock()
			skip := pass == 0 && now.Before(ep.downUntil)
			s.mu.Unlock()
			if skip {
				continue
			}
			tried++
			res, err := ep.query(ctx, expr, ts)
			if err == nil {
				s.mu.Lock()
				s.lastOK, s.lastUsed, s.consecutive = c.now(), ep.cfg.Name, 0
				s.okCount++
				s.mu.Unlock()
				return res, nil
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			errs = append(errs, ep.cfg.Name+": "+err.Error())
			var qe *queryError
			if !errors.As(err, &qe) { // transport-level failure: back off this endpoint
				s.mu.Lock()
				ep.downUntil = c.now().Add(c.cooldown)
				s.mu.Unlock()
			} else {
				// The endpoint answered but rejected the query; another endpoint
				// will not do better.
				break
			}
		}
	}
	err := errors.New(strings.Join(errs, "; "))
	s.mu.Lock()
	s.lastErr, s.lastErrAt = err.Error(), c.now()
	s.errCount++
	s.consecutive++
	s.mu.Unlock()
	return nil, err
}

// Health reports per-source status. A source is unhealthy after 3 consecutive
// failed queries or when nothing succeeded in the last 5 minutes.
func (c *Collector) Health() []Health {
	var out []Health
	now := c.now()
	for _, s := range c.sources {
		s.mu.Lock()
		h := Health{
			Source: s.name, Endpoint: s.lastUsed, LastOK: s.lastOK,
			LastError: s.lastErr, LastErrorAt: s.lastErrAt, Queries: s.okCount + s.errCount, Errors: s.errCount,
		}
		h.Healthy = s.consecutive < 3 && !s.lastOK.IsZero() && now.Sub(s.lastOK) < 5*time.Minute
		s.mu.Unlock()
		out = append(out, h)
	}
	return out
}

type queryError struct{ msg string }

func (e *queryError) Error() string { return e.msg }

type promResponse struct {
	Status    string          `json:"status"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
	Data      json.RawMessage `json:"data"`
}

type promData struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

type promVector []struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

func promTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', 3, 64)
}

// post sends a form to an API path and returns the "data" of a successful response.
func (ep *endpoint) post(ctx context.Context, path string, form url.Values) (*promData, error) {
	u := strings.TrimRight(ep.cfg.URL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "statusd")
	if ep.token != "" {
		req.Header.Set("Authorization", "Bearer "+ep.token)
	}
	resp, err := ep.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("HTTP %d: not a Prometheus response", resp.StatusCode)
	}
	if pr.Status != "success" {
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
			return nil, &queryError{msg: fmt.Sprintf("%s: %s", pr.ErrorType, pr.Error)}
		}
		return nil, fmt.Errorf("HTTP %d: %s %s", resp.StatusCode, pr.ErrorType, pr.Error)
	}
	var d promData
	if err := json.Unmarshal(pr.Data, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func (ep *endpoint) query(ctx context.Context, expr string, ts time.Time) ([]Sample, error) {
	d, err := ep.post(ctx, "/api/v1/query", url.Values{"query": {expr}, "time": {promTime(ts)}})
	if err != nil {
		return nil, err
	}
	switch d.ResultType {
	case "vector":
		var v promVector
		if err := json.Unmarshal(d.Result, &v); err != nil {
			return nil, err
		}
		out := make([]Sample, 0, len(v))
		for _, s := range v {
			f, err := parseValue(s.Value[1])
			if err != nil {
				return nil, err
			}
			out = append(out, Sample{Labels: s.Metric, Value: f})
		}
		return out, nil
	case "scalar":
		var v [2]any
		if err := json.Unmarshal(d.Result, &v); err != nil {
			return nil, err
		}
		f, err := parseValue(v[1])
		if err != nil {
			return nil, err
		}
		return []Sample{{Labels: map[string]string{}, Value: f}}, nil
	default:
		return nil, &queryError{msg: "unsupported result type " + d.ResultType + " (use an instant vector)"}
	}
}

func parseValue(v any) (float64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected sample value %v", v)
	}
	switch s {
	case "NaN":
		return math.NaN(), nil
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(s, 64)
}

// Series is one series of a range query.
type Series struct {
	Labels map[string]string
	Points map[int64]float64 // unix seconds -> value
}

// QueryRange runs a range query against the named source, trying its endpoints
// in order. Used by the replay tool, not by the live evaluation.
func (c *Collector) QueryRange(ctx context.Context, sourceName, expr string, start, end time.Time, step time.Duration) ([]Series, error) {
	s, ok := c.sources[sourceName]
	if !ok {
		return nil, fmt.Errorf("unknown source %q", sourceName)
	}
	form := url.Values{"query": {expr}, "start": {promTime(start)}, "end": {promTime(end)},
		"step": {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)}}
	var errs []string
	for _, ep := range s.endpoints {
		d, err := ep.post(ctx, "/api/v1/query_range", form)
		if err != nil {
			errs = append(errs, ep.cfg.Name+": "+err.Error())
			var qe *queryError
			if errors.As(err, &qe) || ctx.Err() != nil {
				break
			}
			continue
		}
		if d.ResultType != "matrix" {
			return nil, fmt.Errorf("unexpected result type %s", d.ResultType)
		}
		var m []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		}
		if err := json.Unmarshal(d.Result, &m); err != nil {
			return nil, err
		}
		out := make([]Series, 0, len(m))
		for _, r := range m {
			sr := Series{Labels: r.Metric, Points: make(map[int64]float64, len(r.Values))}
			for _, v := range r.Values {
				t, ok := v[0].(float64)
				if !ok {
					return nil, fmt.Errorf("unexpected timestamp %v", v[0])
				}
				f, err := parseValue(v[1])
				if err != nil {
					return nil, err
				}
				sr.Points[int64(t)] = f
			}
			out = append(out, sr)
		}
		return out, nil
	}
	return nil, errors.New(strings.Join(errs, "; "))
}
