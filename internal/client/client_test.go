package client

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNormalizeLocalURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "port", raw: "3000", want: "http://127.0.0.1:3000"},
		{name: "http URL", raw: "http://localhost:8080/app", want: "http://localhost:8080/app"},
		{name: "https URL", raw: "https://localhost:8443", want: "https://localhost:8443"},
		{name: "unsupported scheme", raw: "ftp://localhost:21", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeLocalURL(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeLocalURL() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeLocalURL() error = %v", err)
			}
			if got.String() != test.want {
				t.Fatalf("normalizeLocalURL() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestLocalHTTPClientPreservesLoginRedirectAndCookie(t *testing.T) {
	redirectRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.URL.Path == "/login" {
			http.SetCookie(writer, &http.Cookie{Name: "session", Value: "signed-in"})
			http.Redirect(writer, request, "/", http.StatusFound)
			return
		}
		redirectRequests++
		writer.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	response, err := localHTTPClient.Get(server.URL + "/login")
	if err != nil {
		t.Fatalf("localHTTPClient.Get() error = %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusFound)
	}
	if response.Header.Get("Location") != "/" {
		t.Fatalf("Location = %q, want %q", response.Header.Get("Location"), "/")
	}
	if response.Header.Get("Set-Cookie") == "" {
		t.Fatal("Set-Cookie header is missing")
	}
	if redirectRequests != 0 {
		t.Fatalf("redirect target requests = %d, want 0", redirectRequests)
	}
}
