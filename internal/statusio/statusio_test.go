package statusio

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"statusd/internal/catalog"
	"statusd/internal/model"
	"statusd/internal/view"
)

const (
	page   = "aaaaaaaaaaaaaaaaaaaaaaaa"
	compA  = "a00000000000000000000001" // group "Core Services"
	compB  = "b00000000000000000000002" // group "Mirrors"
	ctDB   = "c00000000000000000000001" // shared container "Database" (in both components)
	ctAuth = "c00000000000000000000002"
	ctWeb  = "c00000000000000000000003"
	compW  = "d00000000000000000000004" // group "Website"
)

// fake is a minimal Status.io API.
type fake struct {
	mu       sync.Mutex
	status   map[Pair]int
	updates  []map[string]any
	times    []time.Time
	failNext int // answer this many requests with HTTP 503
	reject   map[Pair]bool
	headers  http.Header
}

func newFake() *fake {
	return &fake{status: map[Pair]int{
		{compA, ctDB}: 100, {compA, ctAuth}: 100, {compB, ctDB}: 100, {compW, ctWeb}: 100,
	}, reject: map[Pair]bool{}}
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.times = append(f.times, time.Now())
	f.headers = r.Header.Clone()
	if f.failNext > 0 {
		f.failNext--
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	ok := func(result any) {
		json.NewEncoder(w).Encode(map[string]any{"status": map[string]string{"error": "no", "message": "OK"}, "result": result})
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/status/summary/"+page:
		comps := map[string][]map[string]any{}
		for p, code := range f.status {
			comps[p.Component] = append(comps[p.Component], map[string]any{"id": p.Container, "status_code": code})
		}
		var out []map[string]any
		for id, cts := range comps {
			out = append(out, map[string]any{"id": id, "containers": cts})
		}
		ok(map[string]any{"status": out})
	case r.Method == http.MethodGet && r.URL.Path == "/component/list/"+page:
		ok([]map[string]any{
			{"_id": compA, "name": "Core Services (formerly Platform)", "containers": []map[string]any{
				{"_id": ctAuth, "name": "Authentication (Auth)"}, {"_id": ctDB, "name": "Database (DB)"}}},
			{"_id": compB, "name": "Mirrors", "containers": []map[string]any{
				{"_id": ctDB, "name": "Database (DB)"}}},
			{"_id": compW, "name": "Website", "containers": []map[string]any{{"_id": ctWeb, "name": "Website Operational"}}},
		})
	case r.Method == http.MethodPost && r.URL.Path == "/component/status/update":
		var body map[string]any
		b, _ := io.ReadAll(r.Body)
		json.Unmarshal(b, &body)
		p := Pair{body["component"].(string), body["container"].(string)}
		if f.reject[p] {
			json.NewEncoder(w).Encode(map[string]any{"status": map[string]string{"error": "yes", "message": "Invalid container"}})
			return
		}
		f.updates = append(f.updates, body)
		f.status[p] = int(body["current_status"].(float64))
		ok(true)
	default:
		http.NotFound(w, r)
	}
}

func (f *fake) pushes() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.updates...)
}

func loadCat(t *testing.T) *catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"10.yml": `group: {slug: core, name: "Core Services (formerly Platform)", order: 10}
components:
  - {slug: db, name: "Database (DB)", statusio: {component: ` + compA + `, container: ` + ctDB + `}}
  - {slug: auth, name: "Authentication (Auth)", statusio: {component: ` + compA + `, container: ` + ctAuth + `}}
  - {slug: hidden, name: "Hidden", hidden: true, statusio: {component: ` + compW + `, container: ` + ctWeb + `}}
`,
		"20.yml": `group: {slug: mirrors, name: "Mirrors", order: 20}
components:
  - {slug: db, name: "Database (DB)", statusio: {component: ` + compB + `, container: ` + ctDB + `}}
  - {slug: unmapped, name: "Not on Status.io"}
`,
	}
	for n, c := range files {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cat, err := catalog.Load(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	return cat
}

type harness struct {
	f      *fake
	p      *Publisher
	mu     sync.Mutex
	status map[string]model.Status
	leader bool
}

func newHarness(t *testing.T, dryRun bool, withCreds bool) *harness {
	t.Helper()
	f := newFake()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg := Config{Enabled: true, DryRun: &dryRun, BaseURL: srv.URL, StatuspageID: page, MinGap: time.Millisecond, APIIDEnv: "X", APIKeyEnv: "Y"}
	if err := cfg.Normalize(); err != nil {
		t.Fatal(err)
	}
	id, key := "", ""
	if withCreds {
		id, key = "the-id", "the-key"
	}
	h := &harness{f: f, status: map[string]model.Status{}, leader: true}
	resolve := func(time.Time) (map[string]view.ComponentStatus, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		out := map[string]view.ComponentStatus{}
		for id, s := range h.status {
			out[id] = view.ComponentStatus{Status: s}
		}
		return out, nil
	}
	isLeader := func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.leader }
	h.p = NewPublisher(cfg, NewClient(cfg, id, key), loadCat(t), resolve, isLeader, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h.p.active, h.p.pushed = true, map[Pair]int{}
	return h
}

func (h *harness) set(id string, s model.Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.status[id] = s
}

func TestCodeMapping(t *testing.T) {
	want := map[model.Status]int{model.Operational: 100, model.UnderMaintenance: 200, model.DegradedPerformance: 300,
		model.PartialOutage: 400, model.MajorOutage: 500, "": 100}
	for s, c := range want {
		if got := Code(s); got != c {
			t.Errorf("Code(%q) = %d, want %d", s, got, c)
		}
	}
}

func TestSyncPushesOnlyChangesPerPair(t *testing.T) {
	h := newHarness(t, false, true)
	ctx := context.Background()

	h.p.Sync(ctx) // everything operational on both sides: nothing to push
	if n := len(h.f.pushes()); n != 0 {
		t.Fatalf("pushed %d with nothing changed", n)
	}

	h.set("core/db", model.MajorOutage)
	h.p.Sync(ctx)
	ps := h.f.pushes()
	if len(ps) != 1 {
		t.Fatalf("want 1 push, got %d: %v", len(ps), ps)
	}
	got := ps[0]
	if got["component"] != compA || got["container"] != ctDB || got["current_status"].(float64) != 500 ||
		got["statuspage_id"] != page || got["details"] != DefaultDetails[model.MajorOutage] {
		t.Fatalf("bad push %v", got)
	}
	// The shared container under Mirrors is a different pair and stays as it is.
	if h.f.status[Pair{compB, ctDB}] != 100 {
		t.Fatal("mirrors/db changed")
	}
	if hd := h.f.headers; hd.Get("x-api-id") != "the-id" || hd.Get("x-api-key") != "the-key" {
		t.Fatal("auth headers missing")
	}

	h.p.Sync(ctx) // no change: no push
	if n := len(h.f.pushes()); n != 1 {
		t.Fatalf("re-pushed an unchanged status (%d pushes)", n)
	}
	if st := h.p.Status(); st.Pending != 0 || st.Mapped != 3 {
		t.Fatalf("status %+v", st)
	}
}

func TestHiddenAndUnmappedAreSkipped(t *testing.T) {
	h := newHarness(t, false, true)
	h.set("core/hidden", model.MajorOutage)
	h.set("mirrors/unmapped", model.MajorOutage)
	h.p.Sync(context.Background())
	if n := len(h.f.pushes()); n != 0 {
		t.Fatalf("pushed %d for hidden/unmapped components", n)
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	h := newHarness(t, true, true)
	h.set("core/auth", model.PartialOutage)
	h.p.Sync(context.Background())
	if n := len(h.f.pushes()); n != 0 {
		t.Fatalf("dry run pushed %d", n)
	}
	st := h.p.Status()
	if len(st.Recent) != 1 || !st.Recent[0].DryRun || st.Recent[0].Code != 400 {
		t.Fatalf("dry run not recorded: %+v", st.Recent)
	}
	h.p.Sync(context.Background()) // logged once, not every sync
	if n := len(h.p.Status().Recent); n != 1 {
		t.Fatalf("dry run logged %d times", n)
	}
}

func TestDryRunWithoutCredentials(t *testing.T) {
	h := newHarness(t, true, false)
	h.p.Sync(context.Background())
	if n := len(h.f.times); n != 0 {
		t.Fatalf("made %d requests without credentials", n)
	}
	if n := len(h.p.Status().Recent); n != 3 {
		t.Fatalf("want every mapped pair logged once, got %d", n)
	}
}

func TestTransientErrorBacksOffAndRetries(t *testing.T) {
	h := newHarness(t, false, true)
	ctx := context.Background()
	now := time.Now()
	h.p.now = func() time.Time { return now }
	h.p.Sync(ctx) // reconcile
	h.f.mu.Lock()
	h.f.failNext = 1
	h.f.mu.Unlock()
	h.set("core/db", model.DegradedPerformance)
	h.p.Sync(ctx)
	if n := len(h.f.pushes()); n != 0 {
		t.Fatal("push should have failed")
	}
	h.p.Sync(ctx) // still backing off: no request
	if n := len(h.f.pushes()); n != 0 {
		t.Fatal("retried during backoff")
	}
	now = now.Add(16 * time.Second)
	h.p.Sync(ctx)
	if n := len(h.f.pushes()); n != 1 {
		t.Fatalf("not retried after backoff (%d)", n)
	}
	if v := counterValue(h.p.requests.WithLabelValues("component/status/update", "error")); v != 1 {
		t.Fatalf("error counter %v", v)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	h := newHarness(t, false, true)
	ctx := context.Background()
	h.f.reject[Pair{compA, ctAuth}] = true
	h.set("core/auth", model.MajorOutage)
	h.set("core/db", model.MajorOutage)
	h.p.Sync(ctx)
	h.p.Sync(ctx)
	if n := len(h.f.pushes()); n != 1 { // db went through, auth was rejected once
		t.Fatalf("want 1 accepted push, got %d", n)
	}
	rejected := 0
	for _, x := range h.p.Status().Recent {
		if x.Error != "" {
			rejected++
		}
	}
	if rejected != 1 {
		t.Fatalf("rejected push tried %d times", rejected)
	}
	// A new desired status is tried again.
	delete(h.f.reject, Pair{compA, ctAuth})
	h.set("core/auth", model.Operational)
	h.p.Sync(ctx)
	if h.f.status[Pair{compA, ctAuth}] != 100 {
		t.Fatal("changed status not retried")
	}
}

func TestDriftIsDetectedAndOverwritten(t *testing.T) {
	h := newHarness(t, false, true)
	ctx := context.Background()
	now := time.Now()
	h.p.now = func() time.Time { return now }
	h.set("core/db", model.DegradedPerformance)
	h.p.Sync(ctx)
	// Someone sets the pair by hand in the Status.io dashboard.
	h.f.mu.Lock()
	h.f.status[Pair{compA, ctDB}] = 100
	h.f.mu.Unlock()
	now = now.Add(11 * time.Minute) // next reconcile
	h.p.Sync(ctx)
	if v := counterValue(h.p.drift); v != 1 {
		t.Fatalf("drift = %v", v)
	}
	if h.f.status[Pair{compA, ctDB}] != 300 {
		t.Fatal("drift not overwritten")
	}
}

func TestNewLeaderReconcilesFirst(t *testing.T) {
	h := newHarness(t, false, true)
	// The previous leader already pushed the outage before it died.
	h.f.status[Pair{compA, ctDB}] = 500
	h.set("core/db", model.MajorOutage)
	ctx, cancel := context.WithCancel(context.Background())
	h.p.cfg.SyncEvery = 5 * time.Millisecond
	done := make(chan struct{})
	go func() { h.p.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done
	if n := len(h.f.pushes()); n != 0 {
		t.Fatalf("new leader re-pushed %d already-published statuses", n)
	}
}

func TestFollowerDoesNotPublish(t *testing.T) {
	h := newHarness(t, false, true)
	h.leader = false
	h.set("core/db", model.MajorOutage)
	ctx, cancel := context.WithCancel(context.Background())
	h.p.cfg.SyncEvery = 5 * time.Millisecond
	done := make(chan struct{})
	go func() { h.p.Run(ctx); close(done) }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	<-done
	if n := len(h.f.times); n != 0 {
		t.Fatalf("follower made %d requests", n)
	}
}

func TestMinGap(t *testing.T) {
	f := newFake()
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, StatuspageID: page, MinGap: 100 * time.Millisecond}
	cfg.Normalize()
	c := NewClient(cfg, "i", "k")
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() { c.UpdateStatus(context.Background(), Pair{compA, ctDB}, 100, "x") })
	}
	wg.Wait()
	for i := 1; i < len(f.times); i++ {
		if d := f.times[i].Sub(f.times[i-1]); d < 95*time.Millisecond {
			t.Fatalf("requests %d and %d only %v apart", i-1, i, d)
		}
	}
}

func TestErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/component/list/" + page:
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"status":{"error":"yes","message":"Unauthorized"}}`))
		default:
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, StatuspageID: page}
	cfg.Normalize()
	c := NewClient(cfg, "i", "k")
	_, err := c.Components(context.Background())
	if !IsPermanent(err) || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("401: %v", err)
	}
	_, err = c.Summary(context.Background())
	if err == nil || IsPermanent(err) {
		t.Fatalf("502 must be transient: %v", err)
	}
}

func TestMatch(t *testing.T) {
	f := newFake()
	srv := httptest.NewServer(f)
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, StatuspageID: page, MinGap: time.Millisecond}
	cfg.Normalize()
	comps, err := NewClient(cfg, "i", "k").Components(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "10.yml"), []byte(`group: {slug: core, name: "Core Services", order: 10}
components:
  - {slug: db, name: "Database (DB)"}
  - {slug: auth, name: "authentication"}
  - {slug: new, name: "Something new"}
`), 0o644)
	os.WriteFile(filepath.Join(dir, "20.yml"), []byte(`group: {slug: mirrors, name: "Mirrors", order: 20}
components:
  - {slug: db, name: "Database (DB)"}
`), 0o644)
	os.WriteFile(filepath.Join(dir, "30.yml"), []byte(`group: {slug: web, name: "Website", order: 30}
components:
  - {slug: site, name: "example.com"}
`), 0o644)
	cat, err := catalog.Load(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	res := Match(cat, comps)
	want := map[string]Pair{
		"core/db": {compA, ctDB}, "core/auth": {compA, ctAuth},
		"mirrors/db": {compB, ctDB}, "web/site": {compW, ctWeb},
	}
	for id, p := range want {
		if res.Matched[id] != p {
			t.Errorf("%s: got %v, want %v", id, res.Matched[id], p)
		}
	}
	if len(res.Unmatched) != 1 || !strings.Contains(res.Unmatched[0], "Something new") {
		t.Errorf("unmatched %v", res.Unmatched)
	}
	var sb strings.Builder
	WriteMapping(&sb, cat, res)
	if !strings.Contains(sb.String(), "statusio: {component: "+compB+", container: "+ctDB+"}") {
		t.Errorf("mapping output:\n%s", sb.String())
	}
}

func TestConfigValidation(t *testing.T) {
	no := false
	cases := []struct {
		cfg Config
		ok  bool
	}{
		{Config{}, true}, // disabled
		{Config{Enabled: true, StatuspageID: page}, true},               // dry run needs no credentials
		{Config{Enabled: true, StatuspageID: "nope"}, false},            // bad page ID
		{Config{Enabled: true, StatuspageID: page, DryRun: &no}, false}, // live without credentials
		{Config{Enabled: true, StatuspageID: page, DryRun: &no, APIIDEnv: "A", APIKeyFile: "/k"}, true},
		{Config{Details: map[model.Status]string{"broken": "x"}}, false},
	}
	for i, c := range cases {
		if err := c.cfg.Normalize(); (err == nil) != c.ok {
			t.Errorf("case %d: err=%v, want ok=%v", i, err, c.ok)
		}
	}
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	c.Write(&m)
	return m.GetCounter().GetValue()
}
