// Command statusd is a self-hosted, Prometheus-driven status page backend. The same binary runs
// as a full node (evaluates, serves the public API and the control panel) or
// as a witness (raft vote only).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"statusd/internal/admin"
	"statusd/internal/api"
	"statusd/internal/catalog"
	"statusd/internal/collector"
	"statusd/internal/node"
	"statusd/internal/statusio"
	"statusd/internal/store"
	"statusd/internal/view"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "statusd.yaml", "config file")
	witness := flag.Bool("witness", false, "run as a raft witness (overrides node.witness)")
	checkOnly := flag.Bool("check", false, "validate config and catalogue, then exit")
	logJSON := flag.Bool("log-json", false, "JSON logs")
	statusioMap := flag.Bool("statusio-map", false, "match the catalogue to the Status.io page in the config, print the statusio: lines, then exit")
	replayFrom := flag.String("replay-from", "", "replay the catalogue against past data from this time (RFC3339), print what would have been published, then exit")
	replayTo := flag.String("replay-to", "", "end of the replay window (RFC3339, default now)")
	replayStep := flag.Duration("replay-step", 0, "replay step (default: evaluation.interval)")
	replayMatch := flag.String("replay-match", "", "replay only components whose id matches this regex")
	replayCatalog := flag.String("replay-catalog", "", "catalogue directory for the replay (default: catalog_dir from the config)")
	replaySignals := flag.Bool("replay-signals", false, "list the signals behind each replayed episode")
	flag.Parse()

	var h slog.Handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	if *logJSON {
		h = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})
	}
	log := slog.New(h)
	slog.SetDefault(log)

	if *replayFrom != "" {
		o := replayOpts{step: *replayStep, match: *replayMatch, catalog: *replayCatalog, signals: *replaySignals, to: time.Now()}
		var err error
		if o.from, err = time.Parse(time.RFC3339, *replayFrom); err == nil && *replayTo != "" {
			o.to, err = time.Parse(time.RFC3339, *replayTo)
		}
		if err == nil {
			err = runReplay(*cfgPath, o, os.Stdout)
		}
		if err != nil {
			log.Error("replay", "err", err)
			os.Exit(1)
		}
		return
	}
	if *statusioMap {
		if err := printStatusioMap(*cfgPath); err != nil {
			log.Error("statusio-map", "err", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*cfgPath, *witness, *checkOnly, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath string, witnessFlag, checkOnly bool, log *slog.Logger) error {
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}
	if witnessFlag {
		cfg.Node.Witness = true
	}
	log = log.With("node", cfg.Node.ID)

	var (
		col *collector.Collector
		cat *catalog.Catalog
	)
	if !cfg.Node.Witness {
		col, err = collector.New(cfg.Sources)
		if err != nil {
			return err
		}
		cat, err = catalog.Load(cfg.CatalogDir, col.Sources())
		if err != nil {
			return err
		}
		n := 0
		for _, g := range cat.Groups {
			n += len(g.Components)
		}
		log.Info("catalogue loaded", "groups", len(cat.Groups), "components", n)
	}
	if checkOnly {
		fmt.Println("config and catalogue OK")
		return nil
	}

	key, err := cfg.clusterKey()
	if err != nil {
		return fmt.Errorf("cluster key: %w", err)
	}
	if len(cfg.Raft.Peers) > 1 && key == "" && !cfg.Raft.TLS.Enabled() {
		log.Warn("multi-node cluster without mTLS or cluster key: the internal API is unauthenticated")
	}

	// A witness waits longer before standing for election, so a surviving full
	// node almost always wins; if the witness does win it hands leadership over.
	var electionTimeout time.Duration
	if cfg.Node.Witness {
		electionTimeout = 6 * time.Second
	}
	st, err := store.Open(store.Config{
		NodeID: cfg.Node.ID, DataDir: cfg.Node.DataDir, HeartbeatTimeout: electionTimeout, ElectionTimeout: electionTimeout,
		Bind: cfg.Raft.Bind, Advertise: cfg.Raft.Advertise, Bootstrap: cfg.Raft.Bootstrap,
		Peers: cfg.Raft.Peers, TLS: &cfg.Raft.TLS, Logger: log,
	})
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	var tlsCfg *tls.Config
	if cfg.Raft.TLS.Enabled() {
		if tlsCfg, err = cfg.Raft.TLS.Load(); err != nil {
			return err
		}
	}
	transfer := cfg.Evaluation.LeadershipTransfer == nil || *cfg.Evaluation.LeadershipTransfer
	nd := node.New(node.Config{
		Witness: cfg.Node.Witness, Interval: cfg.Evaluation.Interval, RetentionDays: cfg.Evaluation.RetentionDays,
		ClusterKey: key, ClientTLS: tlsCfg, TransferPolicy: transfer,
	}, st, col, cat, log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	nd.RegisterMetrics(reg)
	metrics := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	var servers []*http.Server
	errCh := make(chan error, 4)
	serve := func(name, addr string, handler http.Handler, tc *tls.Config) {
		if addr == "" {
			return
		}
		srv := &http.Server{Addr: addr, Handler: handler, TLSConfig: tc, ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 120 * time.Second}
		servers = append(servers, srv)
		go func() {
			log.Info("listening", "server", name, "addr", addr, "tls", tc != nil)
			var err error
			if tc != nil {
				err = srv.ListenAndServeTLS("", "")
			} else {
				err = srv.ListenAndServe()
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("%s server: %w", name, err)
			}
		}()
	}

	var serverTLS *tls.Config
	if tlsCfg != nil {
		serverTLS = tlsCfg.Clone() // http.Server mutates NextProtos; never share with clients
	}
	serve("internal", cfg.Listen.Internal, nd.InternalHandler(), serverTLS)

	if cfg.Node.Witness {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics)
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
			if st.Leader() == "" {
				http.Error(w, "no leader", http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "ok")
		})
		serve("admin", cfg.Listen.Admin, mux, nil)
	} else {
		vb := &view.Builder{Cat: cat, Store: st, PageName: cfg.PageName, StaleAfter: cfg.Public.StaleAfter}
		health := func() (bool, any) {
			s := st.State()
			age := time.Since(time.Unix(s.LastTick, 0))
			ok := s.Leader != "" && s.LastTick > 0 && age < 3*cfg.Evaluation.Interval
			return ok, map[string]any{"node": s.NodeID, "raft_state": s.RaftState, "leader": s.Leader,
				"last_tick_age_seconds": int(age.Seconds()), "version": version}
		}
		pub := &api.Server{View: vb, CORSOrigins: cfg.Public.CORSOrigins, CacheMaxAge: cfg.Public.CacheMaxAge, Health: health, Log: log, StaticDir: cfg.Public.StaticDir}
		serve("public", cfg.Listen.Public, pub.Handler(), nil)

		deps := admin.Deps{Node: nd, Store: st, Cat: cat, View: vb, Log: log, Version: version}
		if cfg.StatusIO.Enabled {
			id, key, err := cfg.StatusIO.Credentials()
			if err != nil {
				return err
			}
			sio := statusio.NewPublisher(cfg.StatusIO, statusio.NewClient(cfg.StatusIO, id, key), cat, vb.Resolve, st.IsLeader, log)
			sio.RegisterMetrics(reg)
			deps.StatusIO = sio
			go sio.Run(ctx)
		}

		adm, err := admin.New(ctx, cfg.Admin, deps)
		if err != nil {
			return fmt.Errorf("admin: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics)
		mux.Handle("/", adm.Handler())
		serve("admin", cfg.Listen.Admin, mux, nil)

		go nd.ProbeSources(ctx)
	}

	go nd.Run(ctx)
	log.Info("statusd started", "version", version, "witness", cfg.Node.Witness, "raft", cfg.Raft.Bind)

	select {
	case <-ctx.Done():
	case err = <-errCh:
		log.Error("server failed", "err", err)
	}
	log.Info("shutting down")
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	for _, s := range servers {
		_ = s.Shutdown(sctx)
	}
	return err
}

// printStatusioMap reads the Status.io page's components and containers and
// prints the statusio: catalogue lines that match by name.
func printStatusioMap(cfgPath string) error {
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		return err
	}
	if cfg.StatusIO.StatuspageID == "" {
		return fmt.Errorf("statusio.statuspage_id is not set in %s", cfgPath)
	}
	id, key, err := cfg.StatusIO.Credentials()
	if err != nil {
		return err
	}
	if id == "" || key == "" {
		return fmt.Errorf("statusio api_id/api_key are not configured")
	}
	cat, err := catalog.Load(cfg.CatalogDir, nil)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	page, err := statusio.NewClient(cfg.StatusIO, id, key).Components(ctx)
	if err != nil {
		return err
	}
	statusio.WriteMapping(os.Stdout, cat, statusio.Match(cat, page))
	return nil
}
