package websocket

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var ErrorNoRequest = errors.New("no request")

type websocketConn interface {
	NextReader() (messageType int, r io.Reader, err error)
	NextWriter(messageType int) (io.WriteCloser, error)
	Close() error
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

type websocketTransport struct {
	sync.Mutex
	socket  websocketConn
	reader  io.Reader
	closing chan bool
}

var upgrader = &websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func TryUpgrade(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	if w == nil || r == nil {
		return nil, ErrorNoRequest
	}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, err
	}
	return NewConn(ws), nil
}

func Dial(urlStr string, requestHeader http.Header) (net.Conn, error) {
	ws, _, err := websocket.DefaultDialer.Dial(urlStr, requestHeader)
	if err != nil {
		return nil, err
	}
	return NewConn(ws), nil
}

func NewConn(ws websocketConn) net.Conn {
	conn := &websocketTransport{
		socket:  ws,
		closing: make(chan bool),
	}
	return conn
}

func (c *websocketTransport) Read(b []byte) (n int, err error) {
	var opCode int
	if c.reader == nil {
		var r io.Reader
		for {
			if opCode, r, err = c.socket.NextReader(); err != nil {
				return
			}
			if opCode != websocket.BinaryMessage && opCode != websocket.TextMessage {
				continue
			}
			c.reader = r
			break
		}
	}

	n, err = c.reader.Read(b)
	if err != nil {
		if err == io.EOF {
			c.reader = nil
			err = nil
		}
	}
	return
}

func (c *websocketTransport) Write(b []byte) (n int, err error) {
	c.Lock()
	defer c.Unlock()

	var w io.WriteCloser
	if w, err = c.socket.NextWriter(websocket.TextMessage); err == nil {
		if n, err = w.Write(b); err == nil {
			err = w.Close()
		}
	}
	return
}

func (c *websocketTransport) Close() error {
	return c.socket.Close()
}

func (c *websocketTransport) LocalAddr() net.Addr {
	return c.socket.LocalAddr()
}

func (c *websocketTransport) RemoteAddr() net.Addr {
	return c.socket.RemoteAddr()
}

func (c *websocketTransport) SetDeadline(t time.Time) (err error) {
	if err = c.socket.SetReadDeadline(t); err == nil {
		err = c.socket.SetWriteDeadline(t)
	}
	return
}

func (c *websocketTransport) SetReadDeadline(t time.Time) error {
	return c.socket.SetReadDeadline(t)
}

func (c *websocketTransport) SetWriteDeadline(t time.Time) error {
	return c.socket.SetWriteDeadline(t)
}
