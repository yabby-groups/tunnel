package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/yabby-groups/tunnel/internal/client"
	"github.com/yabby-groups/tunnel/internal/server"
	"github.com/yabby-groups/tunnel/internal/websocket"
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

func TestFixedSubdomainsCanRunConcurrently(t *testing.T) {
	newLocal := func(body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
	}
	first := newLocal("first")
	defer first.Close()
	second := newLocal("second")
	defer second.Close()

	tunnelServer, err := server.New(server.Config{BaseDomain: "tunnel.test"}, server.StaticAuthenticator{Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(tunnelServer.Handler())
	defer public.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	urls := make(chan string, 2)
	start := func(localURL, subdomain string) chan error {
		done := make(chan error, 1)
		go func() {
			done <- (&client.Tunnel{
				ServerURL: "ws" + strings.TrimPrefix(public.URL, "http") + "/connect",
				Token:     "test-token", LocalURL: localURL, Subdomain: subdomain,
				OnURL: func(value string) { urls <- value },
			}).Run(ctx)
		}()
		return done
	}
	firstDone := start(first.URL, "first")
	secondDone := start(second.URL, "second")

	registered := map[string]bool{}
	for len(registered) != 2 {
		select {
		case value := <-urls:
			registered[value] = true
		case <-time.After(3 * time.Second):
			t.Fatal("fixed tunnels did not register")
		}
	}
	for host, want := range map[string]string{
		"first.tunnel.test":  "first",
		"second.tunnel.test": "second",
	} {
		request, err := http.NewRequest(http.MethodGet, public.URL+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = host
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK || string(body) != want {
			t.Fatalf("host %s response = %d %q, want 200 %q", host, response.StatusCode, body, want)
		}
	}
	cancel()
	for _, done := range []chan error{firstDone, secondDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("fixed tunnel did not stop")
		}
	}
}
