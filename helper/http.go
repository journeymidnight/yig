package helper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// read from ReadCloser and unmarshal to out;
// `out` should be of POINTER type
func ReadJsonBody(body io.ReadCloser, out interface{}) (err error) {
	defer func() {
		_ = body.Close()
	}()
	jsonBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsonBytes, out)
	if err != nil {
		return err
	}
	return nil
}

// set "SO_REUSEADDR" and "SO_REUSEPORT" on socket option and listen
func ReusePortListener(host, port string) (listener net.Listener, err error) {
	config := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var err error
			c.Control(func(fd uintptr) {
				err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET,
					unix.SO_REUSEADDR, 1)
				if err != nil {
					return
				}

				err = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET,
					unix.SO_REUSEPORT, 1)
				if err != nil {
					return
				}
			})
			return err
		},
	}
	return config.Listen(context.Background(), "tcp",
		fmt.Sprintf("%s:%s", host, port))
}
