package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPAuthenticatorAcceptsControlPlaneUserID(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method=%s", r.Method)
		}
		_, _ = fmt.Fprint(w, `{"user_id":"42"}`)
	}))
	defer control.Close()

	userID, err := (HTTPAuthenticator{URL: control.URL}).Authenticate(context.Background(), "credential")
	if err != nil || userID != "42" {
		t.Fatalf("userID=%q err=%v", userID, err)
	}
}

func TestHTTPAuthenticatorRejectsControlPlaneFailure(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer control.Close()

	_, err := (HTTPAuthenticator{URL: control.URL}).Authenticate(context.Background(), "credential")
	if err == nil {
		t.Fatal("expected authentication rejection")
	}
}
