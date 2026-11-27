// Package webhook notifies HTTP endpoints when a component's published status
// changes. Only the raft leader delivers. The last status each webhook was told
// about is kept in raft, so a new leader continues where the old one stopped:
// nothing is missed, and a duplicate is only possible if the leader dies between
// a delivery and its commit (the event id stays the same, so receivers can
// de-duplicate). While an endpoint is down, changes coalesce: it gets one
// "from -> to" per component when it is back, not every flap in between.
package webhook

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// Formats.
const (
	FormatJSON  = "json"  // statusd's own JSON payload (see Payload)
	FormatSlack = "slack" // Slack incoming webhook; also Mattermost, Rocket.Chat, Discord (…/slack)
)

// Config is one webhook. Secrets (the URL itself for Slack-style hooks, the
// signing secret) can come from files or environment variables.
type Config struct {
	Name       string            `yaml:"name"`        // unique; keys the delivery state in raft (do not rename casually)
	URL        string            `yaml:"url"`         // or url_file / url_env
	URLFile    string            `yaml:"url_file"`    // file holding the URL (e.g. a Slack webhook URL, which is a secret)
	URLEnv     string            `yaml:"url_env"`     // environment variable holding the URL
	Format     string            `yaml:"format"`      // json (default) | slack
	SecretFile string            `yaml:"secret_file"` // json only: HMAC-SHA256 signing secret
	SecretEnv  string            `yaml:"secret_env"`
	Headers    map[string]string `yaml:"headers"`    // extra request headers (e.g. Authorization)
	Components string            `yaml:"components"` // regex on component IDs ("group/slug"); default: all
	MinStatus  string            `yaml:"min_status"` // only notify when from or to is at least this bad (e.g. partial_outage)
	Timeout    time.Duration     `yaml:"timeout"`    // per request, default 10s

	url    string
	secret []byte
	filter *regexp.Regexp
}

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$`)

// Load validates the webhooks and reads their secrets.
func Load(hooks []Config, validStatus func(string) bool) ([]*Config, error) {
	seen := map[string]bool{}
	out := make([]*Config, 0, len(hooks))
	for i := range hooks {
		h := hooks[i]
		if !nameRe.MatchString(h.Name) {
			return nil, fmt.Errorf("webhooks[%d]: name %q must be 1-64 letters, digits, '.', '_' or '-'", i, h.Name)
		}
		if seen[h.Name] {
			return nil, fmt.Errorf("webhooks: duplicate name %q", h.Name)
		}
		seen[h.Name] = true
		u, err := secret(h.URL, h.URLFile, h.URLEnv)
		if err != nil {
			return nil, fmt.Errorf("webhook %s: url: %w", h.Name, err)
		}
		if u == "" {
			return nil, fmt.Errorf("webhook %s: one of url, url_file or url_env is required", h.Name)
		}
		pu, err := url.Parse(u)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			return nil, fmt.Errorf("webhook %s: url must be an absolute http(s) URL", h.Name)
		}
		h.url = u
		switch h.Format {
		case "":
			h.Format = FormatJSON
		case FormatJSON, FormatSlack:
		default:
			return nil, fmt.Errorf("webhook %s: format must be %s or %s", h.Name, FormatJSON, FormatSlack)
		}
		s, err := secret("", h.SecretFile, h.SecretEnv)
		if err != nil {
			return nil, fmt.Errorf("webhook %s: secret: %w", h.Name, err)
		}
		if s != "" && h.Format != FormatJSON {
			return nil, fmt.Errorf("webhook %s: secret_file/secret_env only apply to format json", h.Name)
		}
		h.secret = []byte(s)
		if h.Components != "" {
			re, err := regexp.Compile("^(?:" + h.Components + ")$")
			if err != nil {
				return nil, fmt.Errorf("webhook %s: components: %w", h.Name, err)
			}
			h.filter = re
		}
		if h.MinStatus != "" && !validStatus(h.MinStatus) {
			return nil, fmt.Errorf("webhook %s: min_status %q is not a status", h.Name, h.MinStatus)
		}
		if h.Timeout <= 0 {
			h.Timeout = 10 * time.Second
		}
		out = append(out, &h)
	}
	return out, nil
}

// Matches reports whether the webhook covers a component ID.
func (h *Config) Matches(id string) bool { return h.filter == nil || h.filter.MatchString(id) }

// Signed reports whether deliveries carry an HMAC signature.
func (h *Config) Signed() bool { return len(h.secret) > 0 }

func secret(value, file, env string) (string, error) {
	n := 0
	for _, s := range []string{value, file, env} {
		if s != "" {
			n++
		}
	}
	if n > 1 {
		return "", fmt.Errorf("set only one of the value, *_file and *_env")
	}
	switch {
	case file != "":
		b, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	case env != "":
		v := strings.TrimSpace(os.Getenv(env))
		if v == "" {
			return "", fmt.Errorf("environment variable %s is empty", env)
		}
		return v, nil
	}
	return value, nil
}
