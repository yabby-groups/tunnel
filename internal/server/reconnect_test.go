package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/myna-server/tunnel/internal/client"
)

func TestReconnectReusesHostAndForwardsRequests(t *testing.T) {
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/reconnected" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("reconnected"))
	}))
	defer local.Close()

	tunnelServer, err := New(Config{BaseDomain: "tunnel.test"}, StaticAuthenticator{Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	public := httptest.NewServer(tunnelServer.Handler())
	defer public.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	urls := make(chan string, 2)
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

	firstURL := waitForURL(t, urls)
	tunnelServer.mu.RLock()
	session := tunnelServer.users["development"]
	tunnelServer.mu.RUnlock()
	if session == nil {
		t.Fatal("active session missing")
	}
	if err := session.conn.Close(); err != nil {
		t.Fatal(err)
	}

	secondURL := waitForURL(t, urls)
	if secondURL != firstURL {
		t.Fatalf("reconnected URL = %q, want %q", secondURL, firstURL)
	}
	parsed, err := url.Parse(secondURL)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, public.URL+"/reconnected", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = parsed.Host
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
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

func waitForURL(t *testing.T, urls <-chan string) string {
	t.Helper()
	select {
	case value := <-urls:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("tunnel did not register")
		return ""
	}
}
