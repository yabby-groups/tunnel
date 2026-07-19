package server

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type metrics struct {
	requests       atomic.Uint64
	proxyErrors    atomic.Uint64
	activeSessions atomic.Int64
}

func (m *metrics) serveHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# TYPE tunnel_requests_total counter\ntunnel_requests_total %d\n", m.requests.Load())
	fmt.Fprintf(w, "# TYPE tunnel_proxy_errors_total counter\ntunnel_proxy_errors_total %d\n", m.proxyErrors.Load())
	fmt.Fprintf(w, "# TYPE tunnel_active_sessions gauge\ntunnel_active_sessions %d\n", m.activeSessions.Load())
}
