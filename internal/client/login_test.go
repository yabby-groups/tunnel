package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoginPollsUntilDeviceAuthorizationCompletes(t *testing.T) {
	polls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/tunnel/device/authorize":
			_ = json.NewEncoder(w).Encode(DeviceAuthorization{
				DeviceCode: "device-code", UserCode: "ABCD-EFGH",
				VerificationURI: "https://myna.test/tunnel/authorize",
				Interval:        1,
			})
		case "/api/tunnel/device/token":
			polls++
			if polls == 1 {
				w.WriteHeader(http.StatusAccepted)
				return
			}
			_ = json.NewEncoder(w).Encode(tokenResponse{Token: "credential"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	announced := DeviceAuthorization{}
	token, err := Login(context.Background(), server.URL, func(device DeviceAuthorization) {
		announced = device
	})
	if err != nil {
		t.Fatal(err)
	}
	if token != "credential" || announced.UserCode != "ABCD-EFGH" || polls != 2 {
		t.Fatalf("token=%q announced=%+v polls=%d", token, announced, polls)
	}
}
