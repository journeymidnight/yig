package nettimeout

import (
	"github.com/hydrogen18/stoppableListener"
	"net"
	"strconv"
	"time"
)

// Listener wraps a net.Listener, and gives a place to store the timeout
// parameters. On Accept, it will wrap the net.Conn with our own Conn for us.
type Listener struct {
	*stoppableListener.StoppableListener
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.StoppableListener.Accept()

	if err != nil {
		return nil, err
	}
	tc := &Conn{
		Conn:         c,
		ReadTimeout:  l.ReadTimeout,
		WriteTimeout: l.WriteTimeout,
	}
	return tc, nil
}

func (l *Listener) Stop() {
	l.StoppableListener.Stop()
}

// Conn wraps a net.Conn, and sets a deadline for every read
// and write operation.
type Conn struct {
	net.Conn
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func (c *Conn) Read(b []byte) (int, error) {
	err := c.Conn.SetReadDeadline(time.Now().Add(c.ReadTimeout))
	if err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

func (c *Conn) Write(b []byte) (int, error) {
	err := c.Conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	if err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func NewListener(port int, readTimeout, writeTimeout time.Duration) (*Listener, error) {
	originalListener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	sl, err := stoppableListener.New(originalListener)
	if err != nil {
		return nil, err
	}

	tl := &Listener{
		StoppableListener: sl,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
	}
	return tl, nil
}
