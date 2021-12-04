package main

import (
	"fmt"
	"io"
	"net"
)

// endpoint is a relaxation of the net.Conn interface.
// It's useful for HTTP/1.x-based briding, where the TCP side needs to be
// frankensteined from various parts, and the result is not a net.Conn.
type endpoint interface {
	io.ReadWriter
	RemoteAddr() net.Addr
}

// bridge bridges the given endpoints.
func bridge(x, y endpoint) error {
	xyerrChan := make(chan error, 1)
	yxerrChan := make(chan error, 1)
	go func() {
		_, err := io.Copy(x, y)
		xyerrChan <- err
	}()
	go func() {
		_, err := io.Copy(y, x)
		yxerrChan <- err
	}()
	var xyerr, yxerr error
	select {
	case xyerr = <-xyerrChan:
		if xyerr != nil {
			break
		}
		yxerr = <-yxerrChan
	case yxerr = <-yxerrChan:
		if yxerr != nil {
			break
		}
		xyerr = <-xyerrChan
	}
	if xyerr != nil {
		return fmt.Errorf("bridging data from %s to %s: %w",
			y.RemoteAddr(), x.RemoteAddr(), xyerr)
	}
	if yxerr != nil {
		return fmt.Errorf("briding data from %s to %s: %w",
			x.RemoteAddr(), y.RemoteAddr(), yxerr)
	}
	return nil
}
