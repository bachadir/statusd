package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"statusd/internal/admin"
	"statusd/internal/collector"
	"statusd/internal/statusio"
	"statusd/internal/store"
	"statusd/internal/webhook"
)

type Config struct {
	Node struct {
		ID      string `yaml:"id"`
		Witness bool   `yaml:"witness"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"node"`

	PageName string `yaml:"page_name"`

	Listen struct {
		Public   string `yaml:"public"`   // public JSON API (exposed via the tunnel)
		Admin    string `yaml:"admin"`    // control panel + /metrics (internal only)
		Internal string `yaml:"internal"` // cluster API: write forwarding, peer health
	} `yaml:"listen"`

	Raft struct {
		Bind      string          `yaml:"bind"`
		Advertise string          `yaml:"advertise"`
		Bootstrap bool            `yaml:"bootstrap"`
		Peers     []store.Peer    `yaml:"peers"`
		TLS       store.TLSConfig `yaml:"tls"`
	} `yaml:"raft"`

	ClusterKeyFile string `yaml:"cluster_key_file"`
	ClusterKeyEnv  string `yaml:"cluster_key_env"`

	Evaluation struct {
		Interval           time.Duration `yaml:"interval"`
		RetentionDays      int           `yaml:"retention_days"`
		LeadershipTransfer *bool         `yaml:"leadership_transfer"`
	} `yaml:"evaluation"`

	CatalogDir string `yaml:"catalog_dir"`

	Public struct {
		CORSOrigins []string      `yaml:"cors_origins"`
		CacheMaxAge int           `yaml:"cache_max_age"`
		StaleAfter  time.Duration `yaml:"stale_after"`
		StaticDir   string        `yaml:"static_dir"`
		URL         string        `yaml:"url"` // public page URL, linked from webhook messages (optional)
	} `yaml:"public"`

	Sources map[string]collector.SourceConfig `yaml:"sources"`

	Admin admin.Config `yaml:"admin"`

	// StatusIO: optional, push published statuses to a Status.io page (off by default).
	StatusIO statusio.Config `yaml:"statusio"`

	// Webhooks: notify HTTP endpoints of published status changes (full nodes).
	Webhooks []webhook.Config `yaml:"webhooks"`
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded := os.Expand(string(raw), func(k string) string {
		// ${VAR} expansion for deploy-time values; "$$" stays a literal "$".
		if k == "$" {
			return "$"
		}
		return os.Getenv(k)
	})
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if c.Node.ID == "" {
		return nil, fmt.Errorf("config: node.id is required")
	}
	if c.Node.DataDir == "" {
		c.Node.DataDir = "./data/" + c.Node.ID
	}
	if c.PageName == "" {
		c.PageName = "Status"
	}
	if c.Raft.Bind == "" {
		c.Raft.Bind = "127.0.0.1:7000"
	}
	if c.Evaluation.Interval == 0 {
		c.Evaluation.Interval = 60 * time.Second
	}
	if c.Evaluation.RetentionDays == 0 {
		c.Evaluation.RetentionDays = 90
	}
	if c.Public.StaleAfter == 0 {
		c.Public.StaleAfter = 5 * time.Minute
	}
	if c.CatalogDir == "" {
		c.CatalogDir = "./catalog"
	}
	if !c.Node.Witness {
		if c.Listen.Public == "" {
			c.Listen.Public = ":8080"
		}
		if len(c.Sources) == 0 {
			return nil, fmt.Errorf("config: at least one source is required on a full node")
		}
	}
	if err := c.StatusIO.Normalize(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if len(c.Raft.Peers) > 1 {
		if c.Listen.Internal == "" {
			return nil, fmt.Errorf("config: listen.internal is required in a multi-node cluster (write forwarding)")
		}
		voters, full := 0, 0
		for _, p := range c.Raft.Peers {
			voters++
			if !p.Witness {
				full++
				if p.API == "" {
					return nil, fmt.Errorf("config: peer %s needs an api address", p.ID)
				}
			}
		}
		if voters%2 == 0 {
			return nil, fmt.Errorf("config: %d voters cannot form a safe majority; add a witness", voters)
		}
		if full == 0 {
			return nil, fmt.Errorf("config: at least one non-witness peer is required")
		}
	}
	return &c, nil
}

func (c *Config) clusterKey() (string, error) {
	switch {
	case c.ClusterKeyFile != "":
		b, err := os.ReadFile(c.ClusterKeyFile)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	case c.ClusterKeyEnv != "":
		return os.Getenv(c.ClusterKeyEnv), nil
	}
	return "", nil
}
