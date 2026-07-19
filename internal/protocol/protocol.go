package protocol

import "net/http"

// Message is the JSON envelope carried by the persistent client WebSocket.
// Bodies are bounded by the server's configured maximum before being encoded.
type Message struct {
	Type       string      `json:"type"`
	ID         string      `json:"id,omitempty"`
	URL        string      `json:"url,omitempty"`
	Method     string      `json:"method,omitempty"`
	Path       string      `json:"path,omitempty"`
	Header     http.Header `json:"header,omitempty"`
	StatusCode int         `json:"status_code,omitempty"`
	Body       []byte      `json:"body,omitempty"`
	Error      string      `json:"error,omitempty"`
}

const (
	Registered = "registered"
	Request    = "request"
	Response   = "response"
	WSAccept   = "ws_accept"
	WSData     = "ws_data"
	WSClose    = "ws_close"
	Error      = "error"
)
