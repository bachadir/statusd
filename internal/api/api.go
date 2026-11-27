// Package api serves the public, read-only JSON API consumed by the static
// status page. Only this handler is exposed through the Cloudflare tunnel.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"statusd/internal/store"
	"statusd/internal/view"
)

// HealthFunc reports whether this node should receive public traffic.
type HealthFunc func() (ok bool, detail any)

type Server struct {
	View        *view.Builder
	CORSOrigins []string // allowed Origin values; "*" allows any
	CacheMaxAge int      // seconds (default 30)
	Health      HealthFunc
	StaticDir   string // optional: also serve the static frontend (development / fallback)
	Log         *slog.Logger
	now         func() time.Time
}

func (s *Server) Handler() http.Handler {
	if s.now == nil {
		s.now = time.Now
	}
	if s.CacheMaxAge <= 0 {
		s.CacheMaxAge = 30
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/status.json", s.status)
	mux.HandleFunc("GET /api/v1/incidents.json", s.incidents)
	mux.HandleFunc("GET /api/v1/incidents/{id}", s.incident)
	mux.HandleFunc("GET /api/v1/maintenances.json", s.maintenances)
	mux.HandleFunc("GET /api/v1/uptime.json", s.uptime)
	mux.HandleFunc("GET /healthz", s.healthz)
	if s.StaticDir != "" {
		mux.Handle("GET /", http.FileServer(http.Dir(s.StaticDir)))
	}
	mux.HandleFunc("OPTIONS /api/v1/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return s.cors(mux)
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			for _, o := range s.CORSOrigins {
				if o == "*" || strings.EqualFold(o, origin) {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
					w.Header().Set("Access-Control-Max-Age", "86400")
					break
				}
			}
			w.Header().Add("Vary", "Origin")
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) write(w http.ResponseWriter, v any, maxAge int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// stale-if-error lets Cloudflare keep serving the last answer when every
	// origin is down; the page's stale banner then explains the age.
	w.Header().Set("Cache-Control", "public, max-age="+strconv.Itoa(maxAge)+", stale-while-revalidate=30, stale-if-error=86400")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	if err := enc.Encode(v); err != nil && s.Log != nil {
		s.Log.Warn("encode response", "err", err)
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	if s.Log != nil {
		s.Log.Error("api error", "err", err)
	}
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	sum, err := s.View.Summary(s.now())
	if err != nil {
		s.fail(w, err)
		return
	}
	s.write(w, sum, s.CacheMaxAge)
}

// month parses ?month=YYYY-MM (default: current month, UTC).
func (s *Server) month(r *http.Request) (time.Time, time.Time, error) {
	m := r.URL.Query().Get("month")
	now := s.now().UTC()
	from := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	if m != "" {
		t, err := time.Parse("2006-01", m)
		if err != nil {
			return time.Time{}, time.Time{}, errors.New("month must be YYYY-MM")
		}
		from = t
	}
	return from, from.AddDate(0, 1, 0), nil
}

func (s *Server) incidents(w http.ResponseWriter, r *http.Request) {
	from, to, err := s.month(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := s.View.IncidentHistory(from, to)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.write(w, s.history(from, to, items), 60)
}

func (s *Server) maintenances(w http.ResponseWriter, r *http.Request) {
	from, to, err := s.month(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	items, err := s.View.MaintenanceHistory(from, to, s.now())
	if err != nil {
		s.fail(w, err)
		return
	}
	s.write(w, s.history(from, to, items), 60)
}

func (s *Server) history(from, to time.Time, items any) any {
	doc := map[string]any{
		"month":        from.Format("2006-01"),
		"prev":         from.AddDate(0, -1, 0).Format("2006-01"),
		"generated_at": s.now().UTC(),
		"items":        items,
	}
	if to.Before(s.now()) {
		doc["next"] = to.Format("2006-01")
	}
	return doc
}

func (s *Server) incident(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSuffix(r.PathValue("id"), ".json")
	inc, err := s.View.IncidentByID(id)
	if errors.Is(err, store.ErrNotFound) {
		w.Header().Set("Cache-Control", "public, max-age=30")
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	s.write(w, inc, 60)
}

func (s *Server) uptime(w http.ResponseWriter, r *http.Request) {
	days := 90
	if d := r.URL.Query().Get("days"); d != "" {
		v, err := strconv.Atoi(d)
		if err != nil || v < 1 || v > 90 {
			http.Error(w, "days must be 1..90", http.StatusBadRequest)
			return
		}
		days = v
	}
	u, err := s.View.Uptime(s.now(), days)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.write(w, u, 300)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	ok, detail := true, any(nil)
	if s.Health != nil {
		ok, detail = s.Health()
	}
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]any{"ok": ok, "detail": detail})
}
