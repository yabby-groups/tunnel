package server

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/myna-server/tunnel/internal/protocol"
	"github.com/myna-server/tunnel/internal/websocket"
)

type Config struct {
	BaseDomain       string
	MaxRequestBody   int64
	RequestTimeout   time.Duration
	MaxConcurrentReq int
}

type Server struct {
	config   Config
	auth     Authenticator
	mu       sync.RWMutex
	sessions map[string]*session
	users    map[string]*session
	hosts    map[string]string
	metrics  metrics
}

type session struct {
	server   *Server
	userID   string
	host     string
	conn     *websocket.Conn
	writeMu  sync.Mutex
	requests sync.Map // map[string]chan protocol.Message
	seq      atomic.Uint64
	sem      chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func New(config Config, auth Authenticator) (*Server, error) {
	if config.BaseDomain == "" {
		return nil, fmt.Errorf("base domain is required")
	}
	if auth == nil {
		return nil, fmt.Errorf("authenticator is required")
	}
	if config.MaxRequestBody <= 0 {
		config.MaxRequestBody = 32 << 20
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = 60 * time.Second
	}
	if config.MaxConcurrentReq <= 0 {
		config.MaxConcurrentReq = 100
	}
	return &Server{
		config:   config,
		auth:     auth,
		sessions: make(map[string]*session),
		users:    make(map[string]*session),
		hosts:    make(map[string]string),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", s.connect)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/metrics", s.metrics.serveHTTP)
	mux.HandleFunc("/", s.proxy)
	return mux
}

func (s *Server) connect(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" || token == r.Header.Get("Authorization") {
		http.Error(w, "missing bearer credential", http.StatusUnauthorized)
		return
	}
	userID, err := s.auth.Authenticate(r.Context(), token)
	if err != nil {
		http.Error(w, "invalid tunnel credential", http.StatusUnauthorized)
		return
	}

	s.mu.Lock()
	if _, exists := s.users[userID]; exists {
		s.mu.Unlock()
		http.Error(w, "user already has an active tunnel", http.StatusConflict)
		return
	}
	host, found := s.hosts[userID]
	if !found {
		host, err = s.newHostLocked()
		if err != nil {
			s.mu.Unlock()
			http.Error(w, "allocate tunnel host", http.StatusInternalServerError)
			return
		}
		s.hosts[userID] = host
	}
	conn, err := websocket.Upgrade(w, r)
	if err != nil {
		s.mu.Unlock()
		return
	}
	sess := &session{
		server: s, userID: userID, host: host, conn: conn,
		sem: make(chan struct{}, s.config.MaxConcurrentReq), closed: make(chan struct{}),
	}
	s.sessions[host] = sess
	s.users[userID] = sess
	s.metrics.activeSessions.Add(1)
	s.mu.Unlock()

	defer sess.close()
	if err := sess.send(protocol.Message{Type: protocol.Registered, URL: "https://" + host}); err != nil {
		return
	}
	for {
		var msg protocol.Message
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		sess.dispatch(msg)
	}
}

func (s *Server) newHostLocked() (string, error) {
	for range 8 {
		buf := make([]byte, 8)
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		label := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf))
		host := label + "." + strings.ToLower(s.config.BaseDomain)
		if !s.hostAssignedLocked(host) {
			return host, nil
		}
	}
	return "", fmt.Errorf("could not allocate unique hostname")
}

func (s *Server) hostAssignedLocked(host string) bool {
	if _, found := s.sessions[host]; found {
		return true
	}
	for _, assigned := range s.hosts {
		if assigned == host {
			return true
		}
	}
	return false
}

func (s *session) dispatch(msg protocol.Message) {
	switch msg.Type {
	case protocol.Response, protocol.WSAccept, protocol.WSData, protocol.WSClose, protocol.Error:
		if ch, ok := s.requests.Load(msg.ID); ok {
			select {
			case ch.(chan protocol.Message) <- msg:
			case <-s.closed:
			}
		}
	}
}

func (s *session) send(msg protocol.Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.WriteJSON(msg)
}

func (s *session) close() {
	s.once.Do(func() {
		close(s.closed)
		s.conn.Close()
		s.server.mu.Lock()
		delete(s.server.sessions, s.host)
		delete(s.server.users, s.userID)
		s.server.metrics.activeSessions.Add(-1)
		s.server.mu.Unlock()
	})
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	host := strings.ToLower(stripPort(r.Host))
	s.mu.RLock()
	sess := s.sessions[host]
	s.mu.RUnlock()
	if sess == nil {
		http.NotFound(w, r)
		return
	}
	if websocket.IsWebSocketUpgrade(r) {
		s.proxyWebSocket(w, r, sess)
		return
	}

	s.metrics.requests.Add(1)
	select {
	case sess.sem <- struct{}{}:
		defer func() { <-sess.sem }()
	default:
		http.Error(w, "tunnel is busy", http.StatusTooManyRequests)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.config.MaxRequestBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	id := fmt.Sprintf("%d", sess.seq.Add(1))
	reply := make(chan protocol.Message, 1)
	sess.requests.Store(id, reply)
	defer sess.requests.Delete(id)
	if err := sess.send(protocol.Message{
		Type: protocol.Request, ID: id, Method: r.Method, Path: r.URL.RequestURI(),
		Header: filteredHeader(r.Header), Body: body,
	}); err != nil {
		s.metrics.proxyErrors.Add(1)
		http.Error(w, "tunnel unavailable", http.StatusBadGateway)
		return
	}
	select {
	case response := <-reply:
		if response.Type == protocol.Error {
			s.metrics.proxyErrors.Add(1)
			http.Error(w, response.Error, http.StatusBadGateway)
			return
		}
		copyHeader(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(response.Body)
	case <-time.After(s.config.RequestTimeout):
		s.metrics.proxyErrors.Add(1)
		http.Error(w, "tunnel request timed out", http.StatusGatewayTimeout)
	case <-sess.closed:
		s.metrics.proxyErrors.Add(1)
		http.Error(w, "tunnel unavailable", http.StatusServiceUnavailable)
	}
}

func (s *Server) proxyWebSocket(w http.ResponseWriter, r *http.Request, sess *session) {
	id := fmt.Sprintf("ws-%d", sess.seq.Add(1))
	events := make(chan protocol.Message, 32)
	sess.requests.Store(id, events)
	defer sess.requests.Delete(id)
	if err := sess.send(protocol.Message{
		Type: protocol.Request, ID: id, Method: r.Method, Path: r.URL.RequestURI(), Header: r.Header.Clone(),
	}); err != nil {
		http.Error(w, "tunnel unavailable", http.StatusBadGateway)
		return
	}
	select {
	case event := <-events:
		if event.Type != protocol.WSAccept {
			http.Error(w, event.Error, http.StatusBadGateway)
			return
		}
	case <-time.After(s.config.RequestTimeout):
		http.Error(w, "tunnel request timed out", http.StatusGatewayTimeout)
		return
	case <-sess.closed:
		http.Error(w, "tunnel unavailable", http.StatusServiceUnavailable)
		return
	}
	public, err := websocket.Upgrade(w, r)
	if err != nil {
		return
	}
	defer public.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			typ, data, err := public.ReadMessage()
			if err != nil {
				_ = sess.send(protocol.Message{Type: protocol.WSClose, ID: id})
				return
			}
			if err := sess.send(protocol.Message{Type: protocol.WSData, ID: id, StatusCode: typ, Body: data}); err != nil {
				return
			}
		}
	}()
	for {
		select {
		case event := <-events:
			switch event.Type {
			case protocol.WSData:
				if err := public.WriteMessage(event.StatusCode, event.Body); err != nil {
					return
				}
			case protocol.WSClose, protocol.Error:
				return
			}
		case <-done:
			return
		case <-sess.closed:
			return
		}
	}
}

func filteredHeader(header http.Header) http.Header {
	out := header.Clone()
	for _, key := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		out.Del(key)
	}
	return out
}

func copyHeader(dst, src http.Header) {
	for key, values := range filteredHeader(src) {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func stripPort(host string) string {
	value, _, err := net.SplitHostPort(host)
	if err == nil {
		return value
	}
	return host
}
