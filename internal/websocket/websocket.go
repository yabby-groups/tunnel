// Package websocket implements the small RFC 6455 subset needed by tunnel.
// It deliberately keeps the tunnel binary dependency-free.
package websocket

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

const maxFrameSize = 32 << 20

type Conn struct {
	conn    net.Conn
	reader  *bufio.Reader
	mask    bool
	readMu  sync.Mutex
	writeMu sync.Mutex
}

func Upgrade(w http.ResponseWriter, r *http.Request) (*Conn, error) {
	if !IsWebSocketUpgrade(r) {
		return nil, fmt.Errorf("websocket upgrade required")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("response writer does not support hijacking")
	}
	conn, reader, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		conn.Close()
		return nil, fmt.Errorf("missing websocket key")
	}
	accept := acceptKey(key)
	if _, err := fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept); err != nil {
		conn.Close()
		return nil, err
	}
	return &Conn{conn: conn, reader: reader.Reader, mask: false}, nil
}

func IsWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

type Dialer struct {
	TLSClientConfig *tls.Config
}

var DefaultDialer Dialer

func (d Dialer) DialContext(ctx context.Context, rawURL string, header http.Header) (*Conn, *http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, nil, fmt.Errorf("websocket URL must use ws or wss")
	}
	address := u.Host
	if !strings.Contains(address, ":") {
		if u.Scheme == "wss" {
			address += ":443"
		} else {
			address += ":80"
		}
	}
	var conn net.Conn
	dialer := net.Dialer{}
	if u.Scheme == "wss" {
		config := d.TLSClientConfig
		if config == nil {
			config = &tls.Config{}
		} else {
			config = config.Clone()
		}
		if config.ServerName == "" {
			config.ServerName = u.Hostname()
		}
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, config)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", address)
	}
	if err != nil {
		return nil, nil, err
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	request, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	request.Header = header.Clone()
	request.Host = request.Header.Get("Host")
	request.Header.Del("Host")
	if request.Host == "" {
		request.Host = u.Host
	}
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", key)
	if err := request.Write(conn); err != nil {
		conn.Close()
		return nil, nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if response.StatusCode != http.StatusSwitchingProtocols || response.Header.Get("Sec-WebSocket-Accept") != acceptKey(key) {
		conn.Close()
		return nil, response, fmt.Errorf("websocket upgrade rejected: %s", response.Status)
	}
	return &Conn{conn: conn, reader: reader, mask: true}, response, nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) ReadJSON(value any) error {
	for {
		kind, data, err := c.ReadMessage()
		if err != nil {
			return err
		}
		if kind == TextMessage {
			return json.Unmarshal(data, value)
		}
	}
}

func (c *Conn) WriteJSON(value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteMessage(TextMessage, data)
}

func (c *Conn) ReadMessage() (int, []byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		first, err := c.reader.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		second, err := c.reader.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		if first&0x80 == 0 {
			return 0, nil, fmt.Errorf("fragmented websocket frames are unsupported")
		}
		kind := int(first & 0x0f)
		masked := second&0x80 != 0
		length := uint64(second & 0x7f)
		if length == 126 {
			var value uint16
			if err := binary.Read(c.reader, binary.BigEndian, &value); err != nil {
				return 0, nil, err
			}
			length = uint64(value)
		} else if length == 127 {
			if err := binary.Read(c.reader, binary.BigEndian, &length); err != nil {
				return 0, nil, err
			}
		}
		if length > maxFrameSize {
			return 0, nil, fmt.Errorf("websocket frame too large")
		}
		var key [4]byte
		if masked {
			if _, err := io.ReadFull(c.reader, key[:]); err != nil {
				return 0, nil, err
			}
		}
		data := make([]byte, int(length))
		if _, err := io.ReadFull(c.reader, data); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range data {
				data[i] ^= key[i%4]
			}
		}
		switch kind {
		case PingMessage:
			_ = c.WriteMessage(PongMessage, data)
			continue
		case CloseMessage:
			return CloseMessage, data, io.EOF
		case TextMessage, BinaryMessage:
			return kind, data, nil
		default:
			return 0, nil, fmt.Errorf("unsupported websocket frame type %d", kind)
		}
	}
}

func (c *Conn) WriteMessage(kind int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if len(data) > maxFrameSize {
		return fmt.Errorf("websocket frame too large")
	}
	header := []byte{0x80 | byte(kind)}
	length := len(data)
	maskBit := byte(0)
	if c.mask {
		maskBit = 0x80
	}
	switch {
	case length < 126:
		header = append(header, maskBit|byte(length))
	case length <= 65535:
		header = append(header, maskBit|126, byte(length>>8), byte(length))
	default:
		header = append(header, maskBit|127)
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(length))
		header = append(header, encoded[:]...)
	}
	payload := append([]byte(nil), data...)
	if c.mask {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		header = append(header, key[:]...)
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(payload)
	return err
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}
