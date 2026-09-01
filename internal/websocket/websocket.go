// Package websocket provides RFC 6455 connections for tunnel transports.
package websocket

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/gorilla/websocket"
)

const (
	TextMessage   = websocket.TextMessage
	BinaryMessage = websocket.BinaryMessage
	CloseMessage  = websocket.CloseMessage
	PingMessage   = websocket.PingMessage
	PongMessage   = websocket.PongMessage
)

// Conn wraps a WebSocket connection used by the tunnel protocol.
type Conn struct {
	conn *websocket.Conn
}

// Upgrade accepts an incoming WebSocket connection.
//
// Compression is enabled so the public and local tunnel hops negotiate
// permessage-deflate independently while messages are relayed uncompressed.
func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	conn, err := (&websocket.Upgrader{
		EnableCompression: true,
		CheckOrigin: func(*http.Request) bool {
			return true
		},
	}).Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return &Conn{conn: conn}, nil
}

// IsWebSocketUpgrade reports whether a request asks to upgrade to WebSocket.
func IsWebSocketUpgrade(r *http.Request) bool {
	return websocket.IsWebSocketUpgrade(r)
}

// Dialer configures outbound WebSocket connections.
type Dialer struct {
	TLSClientConfig *tls.Config
}

// DefaultDialer establishes outbound WebSocket connections with default TLS settings.
var DefaultDialer Dialer

// DialContext connects to a WebSocket endpoint.
func (d Dialer) DialContext(ctx context.Context, rawURL string, header http.Header) (*Conn, *http.Response, error) {
	requestHeader := header.Clone()
	compressionRequested := requestHeader.Get("Sec-WebSocket-Extensions") != ""
	// Gorilla creates WebSocket handshake headers itself. Forwarding them from
	// the other tunnel hop would duplicate the values or couple the two
	// independent handshakes.
	requestHeader.Del("Connection")
	requestHeader.Del("Upgrade")
	requestHeader.Del("Sec-WebSocket-Key")
	requestHeader.Del("Sec-WebSocket-Version")
	requestHeader.Del("Sec-WebSocket-Extensions")

	dialer := websocket.Dialer{
		TLSClientConfig:   d.TLSClientConfig,
		EnableCompression: compressionRequested,
	}
	conn, response, err := dialer.DialContext(ctx, rawURL, requestHeader)
	if err != nil {
		return nil, response, err
	}
	return &Conn{conn: conn}, response, nil
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// ReadJSON reads a JSON message.
func (c *Conn) ReadJSON(value any) error {
	return c.conn.ReadJSON(value)
}

// WriteJSON writes a JSON message.
func (c *Conn) WriteJSON(value any) error {
	return c.conn.WriteJSON(value)
}

// ReadMessage reads the next complete WebSocket message.
func (c *Conn) ReadMessage() (int, []byte, error) {
	return c.conn.ReadMessage()
}

// WriteMessage writes one complete WebSocket message.
func (c *Conn) WriteMessage(kind int, data []byte) error {
	return c.conn.WriteMessage(kind, data)
}
