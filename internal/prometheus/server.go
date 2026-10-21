package prometheus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-chi/chi/v5"

	"monstermq.io/edge/internal/archive"
	"monstermq.io/edge/internal/auth"
	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/metrics"
	"monstermq.io/edge/internal/stores"
	"monstermq.io/edge/internal/winccoanative"
)

type Deps struct {
	Cfg         *config.Config
	Archives    *archive.Manager
	Metrics     stores.MetricsStore
	Collector   *metrics.Collector
	Auth        *auth.Cache
	NativeNames func() winccoanative.Names
	Logger      *slog.Logger
}

type Server struct {
	deps    Deps
	router  *chi.Mux
	httpSrv *http.Server
	mu      sync.Mutex
}

func New(deps Deps) *Server {
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{deps: deps}
	s.router = s.buildRouter()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(s.authMiddleware)

	r.Get("/metrics", s.handleMetrics)
	r.Get("/api/v1/labels", s.handleLabels)
	r.Post("/api/v1/labels", s.handleLabels)
	r.Get("/api/v1/label/{name}/values", s.handleLabelValues)
	r.Get("/api/v1/series", s.handleSeries)
	r.Post("/api/v1/series", s.handleSeries)
	r.Get("/api/v1/query_range", s.handleQueryRange)
	r.Post("/api/v1/query_range", s.handleQueryRange)
	r.Get("/api/v1/query", s.handleQuery)
	r.Post("/api/v1/query", s.handleQuery)
	r.Get("/", s.handleRoot)

	return r
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type rootResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, rootResponse{
		Status:  "ok",
		Message: "MonsterMQ Prometheus API",
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

// listenAddr returns the host:port to bind, defaulting to 0.0.0.0:3001.
func (s *Server) listenAddr() string {
	port := 3001
	host := "0.0.0.0"
	if s.deps.Cfg != nil {
		if s.deps.Cfg.Prometheus.Port > 0 {
			port = s.deps.Cfg.Prometheus.Port
		}
		host = s.deps.Cfg.Prometheus.ListenAddress()
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (s *Server) Start() error {
	addr := s.listenAddr()
	srv := &http.Server{
		Addr:    addr,
		Handler: s.router,
	}
	s.mu.Lock()
	s.httpSrv = srv
	s.mu.Unlock()

	s.deps.Logger.Info("prometheus server listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpSrv
	s.httpSrv = nil
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}
