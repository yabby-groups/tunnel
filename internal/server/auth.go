package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Authenticator interface {
	Authenticate(context.Context, string) (string, error)
}

type StaticAuthenticator struct{ Token string }

func (a StaticAuthenticator) Authenticate(_ context.Context, token string) (string, error) {
	if a.Token == "" || token != a.Token {
		return "", fmt.Errorf("invalid tunnel credential")
	}
	return "development", nil
}

// HTTPAuthenticator delegates token validation to the myna control plane.
// The endpoint receives {"token":"..."} and returns {"user_id":"..."}.
type HTTPAuthenticator struct {
	URL    string
	Client *http.Client
}

func (a HTTPAuthenticator) Authenticate(ctx context.Context, token string) (string, error) {
	body, err := json.Marshal(struct {
		Token string `json:"token"`
	}{Token: token})
	if err != nil {
		return "", err
	}
	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("validate tunnel credential: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return "", fmt.Errorf("tunnel credential rejected")
	}
	var payload struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("decode credential response: %w", err)
	}
	if strings.TrimSpace(payload.UserID) == "" {
		return "", fmt.Errorf("credential response has no user_id")
	}
	return payload.UserID, nil
}
