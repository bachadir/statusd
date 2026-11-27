// Package statusio feeds a Status.io status page from statusd (optional, see the
// README). statusd stays the source of truth: the
// raft leader pushes each mapped component's published status to Status.io.
package statusio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"statusd/internal/model"
)

const DefaultBaseURL = "https://api.status.io/v2"

// Config is the "statusio:" block of the node config.
type Config struct {
	Enabled      bool   `yaml:"enabled"`
	DryRun       *bool  `yaml:"dry_run"` // default true: log the pushes, send nothing
	BaseURL      string `yaml:"base_url"`
	StatuspageID string `yaml:"statuspage_id"`
	APIIDFile    string `yaml:"api_id_file"`
	APIIDEnv     string `yaml:"api_id_env"`
	APIKeyFile   string `yaml:"api_key_file"`
	APIKeyEnv    string `yaml:"api_key_env"`
	// MinGap: Status.io asks for one request at a time, at least 1 s apart.
	MinGap time.Duration `yaml:"min_gap"`
	// SyncEvery: how often the published statuses are compared with what
	// Status.io shows. Overrides and maintenances show up within this time.
	SyncEvery time.Duration `yaml:"sync_every"`
	// ReconcileEvery: how often Status.io's own summary is re-read, to catch
	// lost pushes and hand edits (drift).
	ReconcileEvery time.Duration `yaml:"reconcile_every"`
	Timeout        time.Duration `yaml:"timeout"`
	// Details: public text sent with each status, keyed by statusd status.
	Details map[model.Status]string `yaml:"details"`
}

func (c *Config) IsDryRun() bool { return c.DryRun == nil || *c.DryRun }

// Normalize fills defaults and validates. Credentials are only required when
// pushes are really sent.
func (c *Config) Normalize() error {
	if c.BaseURL == "" {
		c.BaseURL = DefaultBaseURL
	}
	if c.MinGap <= 0 {
		c.MinGap = time.Second
	}
	if c.SyncEvery <= 0 {
		c.SyncEvery = 15 * time.Second
	}
	if c.ReconcileEvery <= 0 {
		c.ReconcileEvery = 10 * time.Minute
	}
	if c.Timeout <= 0 {
		c.Timeout = 20 * time.Second
	}
	for s := range c.Details {
		if !s.Valid() {
			return fmt.Errorf("statusio.details: unknown status %q", s)
		}
	}
	if !c.Enabled {
		return nil
	}
	if !hex24(c.StatuspageID) {
		return fmt.Errorf("statusio.statuspage_id must be a 24-character hex ID")
	}
	if !c.IsDryRun() {
		if c.APIIDFile == "" && c.APIIDEnv == "" {
			return fmt.Errorf("statusio: api_id_file or api_id_env is required when dry_run is false")
		}
		if c.APIKeyFile == "" && c.APIKeyEnv == "" {
			return fmt.Errorf("statusio: api_key_file or api_key_env is required when dry_run is false")
		}
	}
	return nil
}

// Credentials reads the API ID and key. Both are empty when none are configured.
func (c *Config) Credentials() (id, key string, err error) {
	if id, err = secret(c.APIIDFile, c.APIIDEnv); err != nil {
		return "", "", fmt.Errorf("statusio api id: %w", err)
	}
	if key, err = secret(c.APIKeyFile, c.APIKeyEnv); err != nil {
		return "", "", fmt.Errorf("statusio api key: %w", err)
	}
	return id, key, nil
}

func secret(file, env string) (string, error) {
	switch {
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	case env != "":
		return strings.TrimSpace(os.Getenv(env)), nil
	}
	return "", nil
}

func hex24(s string) bool {
	if len(s) != 24 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

// Status.io status codes.
const (
	CodeOperational = 100
	CodeMaintenance = 200
	CodeDegraded    = 300
	CodePartial     = 400
	CodeDisruption  = 500
	CodeSecurity    = 600
)

// Code maps a statusd status to a Status.io status code.
func Code(s model.Status) int {
	switch s {
	case model.UnderMaintenance:
		return CodeMaintenance
	case model.DegradedPerformance:
		return CodeDegraded
	case model.PartialOutage:
		return CodePartial
	case model.MajorOutage:
		return CodeDisruption
	}
	return CodeOperational
}

// DefaultDetails is the public text sent with each status. It never contains
// signal internals.
var DefaultDetails = map[model.Status]string{
	model.Operational:         "Operating normally.",
	model.UnderMaintenance:    "Planned maintenance is in progress.",
	model.DegradedPerformance: "We are seeing degraded performance and are investigating.",
	model.PartialOutage:       "We are seeing a partial service disruption and are investigating.",
	model.MajorOutage:         "We are seeing a service disruption and are investigating.",
}

// APIError is an error answer from Status.io. Permanent errors (4xx, or an
// error in the response body) are not retried with the same request.
type APIError struct {
	HTTPStatus int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("status.io: HTTP %d: %s", e.HTTPStatus, e.Message)
}

func (e *APIError) Permanent() bool {
	return e.HTTPStatus < 500 && e.HTTPStatus != http.StatusTooManyRequests
}

// IsPermanent reports whether retrying the same request is pointless.
func IsPermanent(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Permanent()
}

// Client talks to the Status.io v2 API. Requests are serialised and spaced at
// least MinGap apart, as Status.io asks.
type Client struct {
	base, page, id, key string
	gap                 time.Duration
	http                *http.Client

	mu   sync.Mutex // one request at a time
	last time.Time
}

func NewClient(cfg Config, id, key string) *Client {
	return &Client{
		base: strings.TrimRight(cfg.BaseURL, "/"), page: cfg.StatuspageID, id: id, key: key,
		gap: cfg.MinGap, http: &http.Client{Timeout: cfg.Timeout},
	}
}

func (c *Client) HasCredentials() bool { return c.id != "" && c.key != "" }
func (c *Client) StatuspageID() string { return c.page }

type envelope struct {
	Status struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	} `json:"status"`
	Result json.RawMessage `json:"result"`
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if wait := c.gap - time.Since(c.last); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	defer func() { c.last = time.Now() }()

	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-id", c.id)
	req.Header.Set("x-api-key", c.key)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return err
	}
	var env envelope
	jerr := json.Unmarshal(raw, &env)
	if resp.StatusCode != http.StatusOK {
		msg := env.Status.Message
		if jerr != nil || msg == "" {
			msg = strings.TrimSpace(string(raw[:min(len(raw), 200)]))
		}
		return &APIError{HTTPStatus: resp.StatusCode, Message: msg}
	}
	if jerr != nil {
		return fmt.Errorf("status.io: bad response: %w", jerr)
	}
	if strings.EqualFold(env.Status.Error, "yes") {
		return &APIError{HTTPStatus: http.StatusBadRequest, Message: env.Status.Message}
	}
	if out != nil {
		if err := json.Unmarshal(env.Result, out); err != nil {
			return fmt.Errorf("status.io: bad result: %w", err)
		}
	}
	return nil
}

// Pair identifies one status on the page: a Status.io component (our group)
// and a container in it (our component). Containers are shared between
// components in Status.io, so a container ID alone is not enough.
type Pair struct {
	Component string `yaml:"component" json:"component"`
	Container string `yaml:"container" json:"container"`
}

func (p Pair) String() string { return p.Component + "-" + p.Container }

// UpdateStatus sets one pair's status (POST /component/status/update).
func (c *Client) UpdateStatus(ctx context.Context, p Pair, code int, details string) error {
	return c.do(ctx, http.MethodPost, "/component/status/update", map[string]any{
		"statuspage_id": c.page, "component": p.Component, "container": p.Container,
		"current_status": code, "details": details,
	}, nil)
}

// Summary returns the current status code of every pair on the page.
func (c *Client) Summary(ctx context.Context) (map[Pair]int, error) {
	var res struct {
		Status []struct {
			ID         string `json:"id"`
			Containers []struct {
				ID         string `json:"id"`
				StatusCode int    `json:"status_code"`
			} `json:"containers"`
		} `json:"status"`
	}
	if err := c.do(ctx, http.MethodGet, "/status/summary/"+c.page, nil, &res); err != nil {
		return nil, err
	}
	out := map[Pair]int{}
	for _, comp := range res.Status {
		for _, ct := range comp.Containers {
			out[Pair{comp.ID, ct.ID}] = ct.StatusCode
		}
	}
	return out, nil
}

// PageComponent is a Status.io component with its containers.
type PageComponent struct {
	ID         string
	Name       string
	Containers []PageContainer
}

type PageContainer struct {
	ID   string
	Name string
}

// Components lists the page's components and containers (GET /component/list).
func (c *Client) Components(ctx context.Context) ([]PageComponent, error) {
	var res []struct {
		ID         string `json:"_id"`
		Name       string `json:"name"`
		Containers []struct {
			ID       string `json:"_id"`
			Name     string `json:"name"`
			Position int    `json:"position"`
		} `json:"containers"`
	}
	if err := c.do(ctx, http.MethodGet, "/component/list/"+c.page, nil, &res); err != nil {
		return nil, err
	}
	out := make([]PageComponent, 0, len(res))
	for _, r := range res {
		pc := PageComponent{ID: r.ID, Name: r.Name}
		for _, ct := range r.Containers {
			pc.Containers = append(pc.Containers, PageContainer{ID: ct.ID, Name: ct.Name})
		}
		out = append(out, pc)
	}
	return out, nil
}
