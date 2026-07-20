package controlplane

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Server exposes the whitelist HTTP API (+ tiny kill UI).
type Server struct {
	engine     *Engine
	mux        *http.ServeMux
	allowReset bool
	cors       []string
}

// ServerOptions configures public API policy.
type ServerOptions struct {
	AllowReset bool
	// CORSOrigins is a comma-separated allowlist (empty = no CORS headers).
	CORSOrigins string
}

// NewServer builds routes around eng.
func NewServer(eng *Engine, opts ServerOptions) *Server {
	s := &Server{
		engine:     eng,
		mux:        http.NewServeMux(),
		allowReset: opts.AllowReset,
	}
	for _, o := range strings.Split(opts.CORSOrigins, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			s.cors = append(s.cors, o)
		}
	}
	s.mux.HandleFunc("/", s.handleIndex)
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/api/nodes", s.handleList)
	s.mux.HandleFunc("/api/nodes/", s.handleNodeAction)
	s.mux.HandleFunc("/api/stream", s.handleStream)
	s.mux.HandleFunc("/api/reset", s.handleReset)
	return s
}

// Handler returns the root handler with optional CORS.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.applyCORS(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.mux.ServeHTTP(w, r)
	})
}

func (s *Server) applyCORS(w http.ResponseWriter, r *http.Request) {
	if len(s.cors) == 0 {
		return
	}
	origin := r.Header.Get("Origin")
	for _, allowed := range s.cors {
		if origin == allowed || allowed == "*" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			return
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, s.engine.Snapshot(r.Context()))
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	s.engine.AddViewer()
	defer s.engine.RemoveViewer()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, err := json.Marshal(s.engine.Snapshot(ctx))
			if err != nil {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.allowReset {
		http.Error(w, "reset disabled on public control plane", http.StatusForbidden)
		return
	}
	if err := s.engine.ResetAll(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"ok": true, "action": "reset"})
}

func (s *Server) handleNodeAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// /api/nodes/{id}/{kill|restart|partition}
	path := strings.TrimPrefix(r.URL.Path, "/api/nodes/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		http.Error(w, "expected /api/nodes/{id}/{kill|restart|partition}", http.StatusBadRequest)
		return
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || id == 0 {
		http.Error(w, "invalid node id", http.StatusBadRequest)
		return
	}
	var action Action
	switch parts[1] {
	case "kill":
		action = ActionKill
	case "restart":
		action = ActionRestart
	case "partition":
		action = ActionPartition
	default:
		http.Error(w, "unknown action (want kill|restart|partition)", http.StatusBadRequest)
		return
	}

	ip := clientIP(r)
	if err := s.engine.Do(r.Context(), ip, id, action); err != nil {
		code := http.StatusBadRequest
		if strings.Contains(err.Error(), "rate limit") || strings.Contains(err.Error(), "cooldown") {
			code = http.StatusTooManyRequests
		}
		http.Error(w, err.Error(), code)
		return
	}
	writeJSON(w, map[string]any{
		"ok": true, "id": id, "action": action,
		"healAfterMs": s.engine.HealAfter().Milliseconds(),
	})
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// ListenAndServe is a convenience for cmd/controlplane.
func ListenAndServe(addr string, eng *Engine, opts ServerOptions) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           NewServer(eng, opts).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Printf("control plane listening on http://%s\n", addr)
	return srv.ListenAndServe()
}
