package store

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Peer struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"` // raft address host:port
	API     string `yaml:"api"`     // internal API base URL (write forwarding), e.g. https://eu1.internal:8443
	Witness bool   `yaml:"witness"`
}

type TLSConfig struct {
	CAFile     string `yaml:"ca_file"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	ServerName string `yaml:"server_name"` // SAN all node certs carry (default "statusd")
}

func (t *TLSConfig) Enabled() bool { return t != nil && t.CertFile != "" }

// Load builds a mutual-TLS config usable as both server and client.
func (t *TLSConfig) Load() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(t.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in %s", t.CAFile)
	}
	sn := t.ServerName
	if sn == "" {
		sn = "statusd"
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   sn,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

type Config struct {
	NodeID    string
	DataDir   string
	Bind      string // raft listen address
	Advertise string // raft address others dial (default Bind)
	Bootstrap bool   // bootstrap the cluster from Peers if there is no state yet
	Peers     []Peer // full membership, including this node
	TLS       *TLSConfig
	Logger    *slog.Logger

	// Tunables (zero = defaults suited to a transatlantic cluster).
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64
}

type Store struct {
	cfg   Config
	raft  *raft.Raft
	fsm   *FSM
	trans raft.Transport
	bolt  *raftboltdb.BoltStore
	log   *slog.Logger
}

// ErrNotLeader is returned by Apply on a follower.
var ErrNotLeader = errors.New("not the raft leader")

func Open(cfg Config) (*Store, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("store: node id required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Advertise == "" {
		cfg.Advertise = cfg.Bind
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	probe := filepath.Join(cfg.DataDir, ".write-test")
	if err := os.WriteFile(probe, nil, 0o600); err != nil {
		return nil, fmt.Errorf("data dir %s is not writable by this user: %w", cfg.DataDir, err)
	}
	os.Remove(probe)
	fsm, err := NewFSM(filepath.Join(cfg.DataDir, "state.db"))
	if err != nil {
		return nil, err
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: os.Stderr})
	// ~90 ms RTT between EU and US: keep timeouts comfortably above it.
	rc.HeartbeatTimeout = orDur(cfg.HeartbeatTimeout, 2*time.Second)
	rc.ElectionTimeout = orDur(cfg.ElectionTimeout, 2*time.Second)
	rc.LeaderLeaseTimeout = rc.HeartbeatTimeout / 2
	rc.CommitTimeout = 100 * time.Millisecond
	rc.SnapshotThreshold = orU64(cfg.SnapshotThreshold, 2048)
	rc.TrailingLogs = orU64(cfg.TrailingLogs, 2048)
	rc.SnapshotInterval = 5 * time.Minute

	bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("raft log store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(cfg.DataDir, "snapshots"), 3, io.Discard)
	if err != nil {
		return nil, err
	}
	trans, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	r, err := raft.NewRaft(rc, fsm, bolt, bolt, snaps, trans)
	if err != nil {
		return nil, err
	}
	s := &Store{cfg: cfg, raft: r, fsm: fsm, trans: trans, bolt: bolt, log: cfg.Logger}

	if cfg.Bootstrap {
		has, err := raft.HasExistingState(bolt, bolt, snaps)
		if err != nil {
			return nil, err
		}
		if !has {
			var servers []raft.Server
			for _, p := range cfg.Peers {
				servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Address), Suffrage: raft.Voter})
			}
			if len(servers) == 0 {
				servers = []raft.Server{{ID: rc.LocalID, Address: trans.LocalAddr(), Suffrage: raft.Voter}}
			}
			if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
				return nil, fmt.Errorf("bootstrap: %w", err)
			}
			s.log.Info("raft cluster bootstrapped", "servers", len(servers))
		}
	}
	return s, nil
}

func orDur(v, d time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return d
}

func orU64(v, d uint64) uint64 {
	if v > 0 {
		return v
	}
	return d
}

func newTransport(cfg Config) (raft.Transport, error) {
	adv, err := net.ResolveTCPAddr("tcp", cfg.Advertise)
	if err != nil {
		return nil, fmt.Errorf("advertise address: %w", err)
	}
	if !cfg.TLS.Enabled() {
		return raft.NewTCPTransport(cfg.Bind, adv, 3, 10*time.Second, io.Discard)
	}
	tc, err := cfg.TLS.Load()
	if err != nil {
		return nil, fmt.Errorf("raft tls: %w", err)
	}
	ln, err := net.Listen("tcp", cfg.Bind)
	if err != nil {
		return nil, err
	}
	sl := &tlsStreamLayer{Listener: tls.NewListener(ln, tc), adv: adv, tls: tc}
	return raft.NewNetworkTransport(sl, 3, 10*time.Second, io.Discard), nil
}

// tlsStreamLayer is a raft.StreamLayer over mutual TLS.
type tlsStreamLayer struct {
	net.Listener
	adv net.Addr
	tls *tls.Config
}

func (t *tlsStreamLayer) Addr() net.Addr { return t.adv }

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(d, "tcp", string(address), t.tls)
}

// Apply replicates a command. It must be called on the leader.
func (s *Store) Apply(cmd Command, timeout time.Duration) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	f := s.raft.Apply(b, timeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return ErrNotLeader
		}
		return err
	}
	if resp, ok := f.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

// ApplyRaw replicates an already-encoded command (used for forwarded writes).
func (s *Store) ApplyRaw(b []byte, timeout time.Duration) error {
	var cmd Command
	if err := json.Unmarshal(b, &cmd); err != nil {
		return verr("bad command: %v", err)
	}
	return s.Apply(cmd, timeout)
}

func (s *Store) IsLeader() bool { return s.raft.State() == raft.Leader }

// Leader returns the leader's server ID ("" if unknown).
func (s *Store) Leader() string {
	_, id := s.raft.LeaderWithID()
	return string(id)
}

func (s *Store) NodeID() string { return s.cfg.NodeID }

func (s *Store) Peer(id string) (Peer, bool) {
	for _, p := range s.cfg.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

func (s *Store) Peers() []Peer { return s.cfg.Peers }

// LeaderCh delivers true/false as this node gains/loses leadership.
func (s *Store) LeaderCh() <-chan bool { return s.raft.LeaderCh() }

// State is a summary for health endpoints and metrics.
type State struct {
	NodeID       string `json:"node_id"`
	RaftState    string `json:"raft_state"`
	Leader       string `json:"leader"`
	Term         string `json:"term"`
	AppliedIndex uint64 `json:"applied_index"`
	CommitIndex  string `json:"commit_index"`
	LastContact  string `json:"last_contact"`
	LastTick     int64  `json:"last_tick"`
	NumPeers     string `json:"num_peers"`
}

func (s *Store) State() State {
	st := s.raft.Stats()
	return State{
		NodeID:       s.cfg.NodeID,
		RaftState:    s.raft.State().String(),
		Leader:       s.Leader(),
		Term:         st["term"],
		AppliedIndex: s.raft.AppliedIndex(),
		CommitIndex:  st["commit_index"],
		LastContact:  st["last_contact"],
		LastTick:     s.LastTick(),
		NumPeers:     st["num_peers"],
	}
}

// LastTick is the timestamp of the newest applied evaluation tick.
func (s *Store) LastTick() int64 {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return s.fsm.lastTick
}

// LastContact with the leader (zero on the leader itself).
func (s *Store) LastContact() time.Time { return s.raft.LastContact() }

// TransferLeadership asks raft to hand leadership to another voter. With id
// empty, raft picks the most up-to-date follower.
func (s *Store) TransferLeadership(id string) error {
	if id == "" {
		return s.raft.LeadershipTransfer().Error()
	}
	p, ok := s.Peer(id)
	if !ok {
		return fmt.Errorf("unknown peer %q", id)
	}
	return s.raft.LeadershipTransferToServer(raft.ServerID(p.ID), raft.ServerAddress(p.Address)).Error()
}

// WaitForLeader blocks until a leader is known or timeout.
func (s *Store) WaitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Leader() != "" {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("no raft leader")
}

// Barrier waits until all preceding log entries are applied locally (leader only).
func (s *Store) Barrier(timeout time.Duration) error { return s.raft.Barrier(timeout).Error() }

// Snapshot forces a raft snapshot (admin/debug).
func (s *Store) Snapshot() error { return s.raft.Snapshot().Error() }

func (s *Store) Close() error {
	err := s.raft.Shutdown().Error()
	if c, ok := s.trans.(io.Closer); ok {
		c.Close()
	}
	s.bolt.Close()
	s.fsm.Close()
	return err
}
