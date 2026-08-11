package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yabby-groups/tunnel/internal/protocol"
	"github.com/yabby-groups/tunnel/internal/websocket"
)

type Tunnel struct {
	ServerURL     string
	Token         string
	LocalURL      string
	Subdomain     string
	temporaryHost string
	OnURL         func(string)
	OnRequest     func(method, path string, status int, elapsed time.Duration)
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
	serverURL, err := url.Parse(t.ServerURL)
	if err != nil {
		return fmt.Errorf("invalid tunnel server URL: %w", err)
	}
	if t.Subdomain != "" {
		query := serverURL.Query()
		query.Set("subdomain", t.Subdomain)
		serverURL.RawQuery = query.Encode()
	} else if t.temporaryHost != "" {
		query := serverURL.Query()
		query.Set("resume_host", t.temporaryHost)
		serverURL.RawQuery = query.Encode()
	}
	conn, _, err := dialer.DialContext(ctx, serverURL.String(), header)
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
	var requests sync.Map // map[string]context.CancelFunc
	defer requests.Range(func(_, value any) bool { value.(context.CancelFunc)(); return true })

	for {
		var msg protocol.Message
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}
		switch msg.Type {
		case protocol.Registered:
			if t.Subdomain == "" {
				publicURL, err := url.Parse(msg.URL)
				if err != nil || publicURL.Hostname() == "" {
					return fmt.Errorf("invalid registered tunnel URL %q", msg.URL)
				}
				t.temporaryHost = strings.ToLower(publicURL.Hostname())
			}
			if t.OnURL != nil {
				t.OnURL(msg.URL)
			}
		case protocol.Request:
			if websocket.IsWebSocketUpgrade(&http.Request{Header: msg.Header}) {
				go t.openWebSocket(ctx, local, msg, send, &sockets)
			} else {
				go t.handleRequest(ctx, local, msg, send, &requests)
			}
		case protocol.Cancel:
			if value, ok := requests.Load(msg.ID); ok {
				value.(context.CancelFunc)()
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

func (t *Tunnel) handleRequest(ctx context.Context, local *url.URL, msg protocol.Message, send func(protocol.Message) error, requests *sync.Map) {
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	requests.Store(msg.ID, cancel)
	defer func() {
		requests.Delete(msg.ID)
		cancel()
	}()
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
	resp, err := localHTTPClient.Do(req)
	if err != nil {
		_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "local service: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	streamResponse, normalizeEventStream := shouldStreamResponse(resp, req.Header)
	if streamResponse {
		header := filteredHeader(resp.Header)
		if normalizeEventStream {
			header.Set("Content-Type", "text/event-stream")
		}
		if err := send(protocol.Message{
			Type: protocol.ResponseStart, ID: msg.ID, StatusCode: resp.StatusCode,
			Header: header,
		}); err != nil {
			return
		}
		buffer := make([]byte, 32*1024)
		for {
			n, readErr := resp.Body.Read(buffer)
			if n > 0 {
				if err := send(protocol.Message{Type: protocol.ResponseData, ID: msg.ID, Body: append([]byte(nil), buffer[:n]...)}); err != nil {
					return
				}
			}
			if readErr == io.EOF {
				_ = send(protocol.Message{Type: protocol.ResponseEnd, ID: msg.ID})
				break
			}
			if readErr != nil {
				_ = send(protocol.Message{Type: protocol.Error, ID: msg.ID, Error: "read local response: " + readErr.Error()})
				break
			}
		}
		if t.OnRequest != nil {
			t.OnRequest(msg.Method, msg.Path, resp.StatusCode, time.Since(start))
		}
		return
	}
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

var localHTTPClient = &http.Client{Transport: func() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 60 * time.Second
	return transport
}()}

func isEventStream(header http.Header) bool {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	return err == nil && strings.EqualFold(mediaType, "text/event-stream")
}

func acceptsEventStream(header http.Header) bool {
	for _, value := range header.Values("Accept") {
		for _, mediaRange := range strings.Split(value, ",") {
			mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(mediaRange))
			if err == nil && strings.EqualFold(mediaType, "text/event-stream") {
				return true
			}
		}
	}
	return false
}

func shouldStreamResponse(response *http.Response, requestHeader http.Header) (stream bool, normalizeEventStream bool) {
	if isEventStream(response.Header) {
		return true, false
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return false, false
	}
	if acceptsEventStream(requestHeader) {
		return true, true
	}
	return isBinaryMediaType(response.Header) || response.ContentLength < 0, false
}

func isBinaryMediaType(header http.Header) bool {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	mediaType = strings.ToLower(mediaType)
	return err == nil && !strings.HasPrefix(mediaType, "text/") && mediaType != "application/json"
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
	if port, err := strconv.ParseUint(raw, 10, 16); err == nil && port > 0 {
		raw = "http://127.0.0.1:" + raw
	}
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

// Subdomain is one stable public tunnel name owned by the current user.
type Subdomain struct {
	ID        int    `json:"id"`
	Subdomain string `json:"subdomain"`
	CreatedAt int64  `json:"created_at"`
}

type tokenResponse struct {
	Token string `json:"token"`
}

func controlRequest(ctx context.Context, controlURL, token, method, path string, payload any, result any) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(
		ctx, method, strings.TrimRight(controlURL, "/")+path, body,
	)
	if err != nil {
		return err
	}
	req.Header.Set("X-REQUEST-TOKEN", token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("tunnel control request failed: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

// ListSubdomains returns the stable tunnel names owned by the credential user.
func ListSubdomains(ctx context.Context, controlURL, token string) ([]Subdomain, error) {
	var response struct {
		Subdomains []Subdomain `json:"subdomains"`
	}
	if err := controlRequest(ctx, controlURL, token, http.MethodGet, "/api/tunnel/subdomains/", nil, &response); err != nil {
		return nil, err
	}
	return response.Subdomains, nil
}

// ClaimSubdomain reserves one stable tunnel name for the credential user.
func ClaimSubdomain(ctx context.Context, controlURL, token, subdomain string) (string, error) {
	var response struct {
		Subdomain string `json:"subdomain"`
	}
	err := controlRequest(ctx, controlURL, token, http.MethodPost, "/api/tunnel/subdomains/", struct {
		Subdomain string `json:"subdomain"`
	}{Subdomain: subdomain}, &response)
	if err != nil {
		return "", err
	}
	if response.Subdomain == "" {
		return "", fmt.Errorf("tunnel control returned an empty subdomain")
	}
	return response.Subdomain, nil
}

// ReleaseSubdomain releases a stable tunnel name owned by the credential user.
func ReleaseSubdomain(ctx context.Context, controlURL, token string, subdomainID int) error {
	var response struct {
		Result string `json:"result"`
	}
	return controlRequest(ctx, controlURL, token, http.MethodPost, "/api/tunnel/subdomains/release/", struct {
		SubdomainID int `json:"subdomain_id"`
	}{SubdomainID: subdomainID}, &response)
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
