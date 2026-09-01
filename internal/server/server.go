package server

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yabby-groups/tunnel/internal/protocol"
	"github.com/yabby-groups/tunnel/internal/websocket"
)

type Config struct {
	BaseDomain       string
	MaxRequestBody   int64
	RequestTimeout   time.Duration
	MaxConcurrentReq int
}

type Server struct {
	config         Config
	auth           Authenticator
	mu             sync.RWMutex
	sessions       map[string]*session
	temporaryHosts map[string]string
	metrics        metrics
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
		config:         config,
		auth:           auth,
		sessions:       make(map[string]*session),
		temporaryHosts: make(map[string]string),
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
	subdomain := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("subdomain")))
	resumeHost := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("resume_host")))
	if subdomain != "" && !validSubdomain(subdomain) {
		http.Error(w, "invalid tunnel subdomain", http.StatusBadRequest)
		return
	}
	if subdomain != "" && resumeHost != "" {
		http.Error(w, "tunnel host request is ambiguous", http.StatusBadRequest)
		return
	}
	userID, err := s.auth.Authenticate(r.Context(), token, subdomain)
	if err != nil {
		http.Error(w, "invalid tunnel credential", http.StatusUnauthorized)
		return
	}

	s.mu.Lock()
	host := ""
	allocatedTemporaryHost := false
	if subdomain == "" && resumeHost == "" {
		host, err = s.newHostLocked()
		if err != nil {
			s.mu.Unlock()
			http.Error(w, "allocate tunnel host", http.StatusInternalServerError)
			return
		}
		allocatedTemporaryHost = true
	} else if resumeHost != "" {
		owner, found := s.temporaryHosts[resumeHost]
		if !found || owner != userID {
			s.mu.Unlock()
			http.Error(w, "temporary tunnel host is unavailable", http.StatusConflict)
			return
		}
		if _, active := s.sessions[resumeHost]; active {
			s.mu.Unlock()
			http.Error(w, "temporary tunnel host is already active", http.StatusConflict)
			return
		}
		host = resumeHost
	} else {
		host = subdomain + "." + strings.ToLower(s.config.BaseDomain)
		if _, exists := s.sessions[host]; exists {
			s.mu.Unlock()
			http.Error(w, "tunnel subdomain is already active", http.StatusConflict)
			return
		}
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
	if allocatedTemporaryHost {
		s.temporaryHosts[host] = userID
	}
	s.sessions[host] = sess
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
	if _, found := s.temporaryHosts[host]; found {
		return true
	}
	return false
}

func validSubdomain(value string) bool {
	if len(value) <= 4 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, char := range value {
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func (s *session) dispatch(msg protocol.Message) {
	switch msg.Type {
	case protocol.Response, protocol.ResponseStart, protocol.ResponseData, protocol.ResponseEnd, protocol.WSAccept, protocol.WSData, protocol.WSClose, protocol.Error:
		if ch, ok := s.requests.Load(msg.ID); ok {
			switch target := ch.(type) {
			case chan protocol.Message:
				select {
				case target <- msg:
				case <-s.closed:
				}
			case *webSocketEventQueue:
				target.Push(msg)
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
	cancel := func() { _ = sess.send(protocol.Message{Type: protocol.Cancel, ID: id}) }
	if err := sess.send(protocol.Message{
		Type: protocol.Request, ID: id, Method: r.Method, Path: r.URL.RequestURI(),
		Host:   r.Host,
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
		if response.Type == protocol.ResponseStart {
			s.proxyStream(w, r, sess, reply, response, cancel)
			return
		}
		copyHeader(w.Header(), response.Header)
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(response.Body)
	case <-time.After(s.config.RequestTimeout):
		cancel()
		s.metrics.proxyErrors.Add(1)
		http.Error(w, "tunnel request timed out", http.StatusGatewayTimeout)
	case <-r.Context().Done():
		cancel()
	case <-sess.closed:
		s.metrics.proxyErrors.Add(1)
		http.Error(w, "tunnel unavailable", http.StatusServiceUnavailable)
	}
}

func (s *Server) proxyStream(w http.ResponseWriter, r *http.Request, sess *session, events <-chan protocol.Message, start protocol.Message, cancel func()) {
	copyHeader(w.Header(), start.Header)
	w.WriteHeader(start.StatusCode)
	flusher, ok := w.(http.Flusher)
	if !ok {
		cancel()
		return
	}
	flusher.Flush()
	for {
		select {
		case event := <-events:
			switch event.Type {
			case protocol.ResponseData:
				if _, err := w.Write(event.Body); err != nil {
					cancel()
					return
				}
				flusher.Flush()
			case protocol.ResponseEnd, protocol.Error:
				return
			}
		case <-r.Context().Done():
			cancel()
			return
		case <-sess.closed:
			return
		}
	}
}

func (s *Server) proxyWebSocket(w http.ResponseWriter, r *http.Request, sess *session) {
	id := fmt.Sprintf("ws-%d", sess.seq.Add(1))
	events := newWebSocketEventQueue()
	sess.requests.Store(id, events)
	defer func() {
		sess.requests.Delete(id)
		events.Close()
	}()
	header := r.Header.Clone()
	// Host is carried separately by net/http. Preserve the public authority so
	// local WebSocket servers can validate the browser's Origin header.
	header.Set("Host", r.Host)
	if err := sess.send(protocol.Message{
		Type: protocol.Request, ID: id, Method: r.Method, Path: r.URL.RequestURI(), Header: header,
	}); err != nil {
		log.Printf("tunnel websocket request relay id=%s: %v", id, err)
		http.Error(w, "tunnel unavailable", http.StatusBadGateway)
		return
	}
	select {
	case event, ok := <-events.Output():
		if !ok {
			log.Printf("tunnel websocket queue closed before local handshake id=%s", id)
			http.Error(w, "tunnel unavailable", http.StatusServiceUnavailable)
			return
		}
		if event.Type != protocol.WSAccept {
			log.Printf("tunnel websocket local handshake rejected id=%s: %s", id, event.Error)
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
		log.Printf("tunnel websocket public handshake id=%s: %v", id, err)
		return
	}
	defer public.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			typ, data, err := public.ReadMessage()
			if err != nil {
				log.Printf("tunnel websocket public read id=%s: %v", id, err)
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
		case event, ok := <-events.Output():
			if !ok {
				log.Printf("tunnel websocket queue closed id=%s", id)
				return
			}
			switch event.Type {
			case protocol.WSData:
				if err := public.WriteMessage(event.StatusCode, event.Body); err != nil {
					log.Printf("tunnel websocket public write id=%s: %v", id, err)
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
