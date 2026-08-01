package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSubdomainManagementUsesCredentialAndExpectedEndpoints(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-REQUEST-TOKEN") != "credential" {
			t.Fatalf("credential header = %q", r.Header.Get("X-REQUEST-TOKEN"))
		}
		switch r.URL.Path {
		case "/api/tunnel/subdomains/":
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte(`{"subdomains":[{"id":3,"subdomain":"demo","created_at":1}]}`))
				return
			}
			var request struct {
				Subdomain string `json:"subdomain"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			if request.Subdomain != "demo" {
				t.Fatalf("claim = %q", request.Subdomain)
			}
			_, _ = w.Write([]byte(`{"subdomain":"demo"}`))
		case "/api/tunnel/subdomains/release/":
			var request struct {
				SubdomainID int `json:"subdomain_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatal(err)
			}
			if request.SubdomainID != 3 {
				t.Fatalf("release id = %d", request.SubdomainID)
			}
			_, _ = w.Write([]byte(`{"result":"OK"}`))
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
	}))
	defer control.Close()

	items, err := ListSubdomains(context.Background(), control.URL, "credential")
	if err != nil || len(items) != 1 || items[0].Subdomain != "demo" {
		t.Fatalf("ListSubdomains() = %#v, %v", items, err)
	}
	name, err := ClaimSubdomain(context.Background(), control.URL, "credential", "demo")
	if err != nil || name != "demo" {
		t.Fatalf("ClaimSubdomain() = %q, %v", name, err)
	}
	if err := ReleaseSubdomain(context.Background(), control.URL, "credential", 3); err != nil {
		t.Fatalf("ReleaseSubdomain() = %v", err)
	}
}
