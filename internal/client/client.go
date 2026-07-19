package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/myna-server/tunnel/internal/protocol"
	"github.com/myna-server/tunnel/internal/websocket"
)

type Tunnel struct {
	ServerURL string
	Token     string
	LocalURL  string
	OnURL     func(string)
	OnRequest func(method, path string, status int, elapsed time.Duration)
}

func (t *Tunnel) Run(ctx context.Context) error {
	local, err := normalizeLocalURL(t.LocalURL)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	for {
		if err := t.runOnce(ctx, dialer, local); err != nil && ctx.Err() == nil {
			log.Printf("tunnel disconnected: %v; reconnecting", err)
		}
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(3 * time.Second):
		}
	}
}

func (t *Tunnel) runOnce(ctx context.Context, dialer websocket.Dialer, local *url.URL) error {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+t.Token)
	conn, _, err := dialer.DialContext(ctx, t.ServerURL, header)
	if err != nil {
		return err
	}
	stopClose := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)
	defer conn.Close()
	var writeMu sync.Mutex
	send := func(msg protocol.Message) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(msg)
	}
	var sockets sync.Map // map[string]*websocket.Conn
	defer sockets.Range(func(_, value any) bool { value.(*websocket.Conn).Close(); return true })

	for {
		var msg protocol.Message
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}
		switch msg.Type {
		case protocol.Registered:
			if t.OnURL != nil {
				t.OnURL(msg.URL)
			}
		case protocol.Request:
			if websocket.IsWebSocketUpgrade(&http.Request{Header: msg.Header}) {
				go t.openWebSocket(ctx, local, msg, send, &sockets)
			} else {
				go t.handleRequest(ctx, local, msg, send)
			}
		case protocol.WSData:
			if value, ok := sockets.Load(msg.ID); ok {
				if err := value.(*websocket.Conn).WriteMessage(msg.StatusCode, msg.Body); err != nil {
					_ = send(protocol.Message{Type: protocol.WSClose, ID: msg.ID})
				}
			}
		case protocol.WSClose:
			if value, ok := sockets.LoadAndDelete(msg.ID); ok {
				value.(*websocket.Conn).Close()
			}
		}
	}
}

func (t *Tunnel) handleRequest(ctx context.Context, local *url.URL, msg protocol.Message, send func(protocol.Message) error) {
	start := time.Now()
	relative, err := url.Parse(msg.Path)
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "invalid request path"})
		return
	}
	target := local.ResolveReference(relative)
	req, err := http.NewRequestWithContext(ctx, msg.Method, target.String(), bytes.NewReader(msg.Body))
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: err.Error()})
		return
	}
	req.Header = filteredHeader(msg.Header)
	req.Host = local.Host
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "local service: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "read local response: " + err.Error()})
		return
	}
	_ = send(protocol.Message{
		Type: protocol.Response, ID: msg.ID, StatusCode: resp.StatusCode,
		Header: filteredHeader(resp.Header), Body: body,
	})
	if t.OnRequest != nil {
		t.OnRequest(msg.Method, msg.Path, resp.StatusCode, time.Since(start))
	}
}

func (t *Tunnel) openWebSocket(ctx context.Context, local *url.URL, msg protocol.Message, send func(protocol.Message) error, sockets *sync.Map) {
	relative, err := url.Parse(msg.Path)
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "invalid websocket path"})
		return
	}
	target := local.ResolveReference(relative)
	if local.Scheme == "https" {
		target.Scheme = "wss"
	} else {
		target.Scheme = "ws"
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, target.String(), filteredHeader(msg.Header))
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "local websocket: " + err.Error()})
		return
	}
	sockets.Store(msg.ID, conn)
	defer func() {
		sockets.Delete(msg.ID)
		conn.Close()
		_ = send(protocol.Message{Type: protocol.WSClose, ID: msg.ID})
	}()
	if err := send(protocol.Message{Type: protocol.WSAccept, ID: msg.ID}); err != nil {
		return
	}
	for {
		typ, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if err := send(protocol.Message{Type: protocol.WSData, ID: msg.ID, StatusCode: typ, Body: data}); err != nil {
			return
		}
	}
}

func normalizeLocalURL(raw string) (*url.URL, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid local URL %q", raw)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("local URL must use http or https")
	}
	return u, nil
}

func filteredHeader(header http.Header) http.Header {
	out := header.Clone()
	for _, key := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		out.Del(key)
	}
	return out
}

type DeviceAuthorization struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	Interval        int    `json:"interval"`
}

type tokenResponse struct {
	Token string `json:"token"`
}

// Login implements the documented myna device-authorization contract.
func Login(ctx context.Context, controlURL string, announce func(DeviceAuthorization)) (string, error) {
	resp, err := http.Post(controlURL+"/api/tunnel/device/authorize", "application/json", nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("start device authorization: %s", resp.Status)
	}
	var device DeviceAuthorization
	if err := json.NewDecoder(resp.Body).Decode(&device); err != nil {
		return "", err
	}
	if device.DeviceCode == "" || device.VerificationURI == "" {
		return "", fmt.Errorf("invalid device authorization response")
	}
	announce(device)
	interval := time.Duration(device.Interval) * time.Second
	if interval <= 0 {
		interval = 3 * time.Second
	}
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
		body, _ := json.Marshal(struct {
			DeviceCode string `json:"device_code"`
		}{DeviceCode: device.DeviceCode})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, controlURL+"/api/tunnel/device/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		if response.StatusCode == http.StatusAccepted {
			response.Body.Close()
			continue
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return "", fmt.Errorf("device authorization rejected: %s", response.Status)
		}
		var token tokenResponse
		err = json.NewDecoder(response.Body).Decode(&token)
		response.Body.Close()
		if err != nil {
			return "", err
		}
		if token.Token == "" {
			return "", fmt.Errorf("device authorization returned empty token")
		}
		return token.Token, nil
	}
}
