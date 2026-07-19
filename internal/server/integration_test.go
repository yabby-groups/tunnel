package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/myna-server/tunnel/internal/client"
	"github.com/myna-server/tunnel/internal/server"
	"github.com/myna-server/tunnel/internal/websocket"
)

func TestHTTPForwarding(t *testing.T) {
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/hello" || r.URL.RawQuery != "name=myna" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Local-Service", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("forwarded"))
	}))
	defer local.Close()

	tunnelServer, err := server.New(server.Config{BaseDomain: "tunnel.test"}, server.StaticAuthenticator{Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(tunnelServer.Handler())
	defer public.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	urls := make(chan string, 1)
	tunnel := client.Tunnel{
		ServerURL: "ws" + strings.TrimPrefix(public.URL, "http") + "/connect",
		Token:     "test-token",
		LocalURL:  local.URL,
		OnURL: func(value string) {
			urls <- value
		},
	}
	done := make(chan error, 1)
	go func() { done <- tunnel.Run(ctx) }()

	var publicURL string
	select {
	case publicURL = <-urls:
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not register")
	}
	parsed, err := url.Parse(publicURL)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, public.URL+"/hello?name=myna", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = parsed.Host
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if response.Header.Get("X-Local-Service") != "yes" {
		t.Fatalf("missing forwarded response header")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not stop")
	}
}

func TestWebSocketForwarding(t *testing.T) {
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Upgrade(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer conn.Close()
		kind, data, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(kind, data)
		}
	}))
	defer local.Close()

	tunnelServer, err := server.New(server.Config{BaseDomain: "tunnel.test"}, server.StaticAuthenticator{Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(tunnelServer.Handler())
	defer public.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	urls := make(chan string, 1)
	tunnel := client.Tunnel{
		ServerURL: "ws" + strings.TrimPrefix(public.URL, "http") + "/connect",
		Token:     "test-token",
		LocalURL:  local.URL,
		OnURL: func(value string) {
			urls <- value
		},
	}
	done := make(chan error, 1)
	go func() { done <- tunnel.Run(ctx) }()

	var publicURL string
	select {
	case publicURL = <-urls:
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not register")
	}
	parsed, err := url.Parse(publicURL)
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(
		ctx, "ws"+strings.TrimPrefix(public.URL, "http")+"/socket",
		http.Header{"Host": []string{parsed.Host}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello websocket")); err != nil {
		t.Fatal(err)
	}
	kind, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.TextMessage || string(data) != "hello websocket" {
		t.Fatalf("unexpected websocket response kind=%d body=%q", kind, data)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client did not stop")
	}
}
