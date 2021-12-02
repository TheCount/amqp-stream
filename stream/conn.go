package stream

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// conn encapsulates an AMQP stream connection (for either client or server).
// A conn must not be copied while in use.
type conn struct {
	// amqpConn is the AMQP connection (if this conn is responsible for closing
	// it).
	// If non-nil, it is closed by Close().
	amqpConn *amqp.Connection

	// inChan and outChan are the AMQP channels for incoming and outgoing data,
	// respectively.
	// inChan is closed by Close if it hasn't been closed by Read
	// (due to EOF from the other end) or Write (due to an unrecoverable
	// publishing error) already.
	// outChan is closed by Close if it hasn't been closed by Write already
	// (due to an unrecoverable publishing error).
	inChan, outChan *amqp.Channel

	// dChan is the AMQP delivery channel.
	dChan <-chan amqp.Delivery

	// localAddr and remoteAddr are the local and remote connection address,
	// respectively.
	localAddr, remoteAddr *addr

	// rBuf is the current read buffer, in case more data was read than can be
	// returned.
	rBuf []byte

	// rChan and wChan synchronize read and writes, respectively.
	rChan, wChan chan struct{}

	// rDeadline and wDeadline handle read and write deadlines.
	rDeadline, wDeadline deadline

	// eofReceived indicates whether the other end has closed its connection.
	eofReceived signalChan

	// closed indicates whether this connection has been closed.
	closed signalChan
}

// Close implements net.Conn.Close.
func (c *conn) Close() error {
	select {
	case c.wChan <- struct{}{}:
		err := c.outChan.Publish("", c.remoteAddr.values.Get("remote_queue"),
			true, false, amqp.Publishing{
				DeliveryMode: amqp.Transient,
			})
		if err2 := c.outChan.Close(); err2 != nil && err == nil {
			err = err2
		}
		select {
		case c.rChan <- struct{}{}:
			if err2 := c.inChan.Close(); err2 != nil && err == nil {
				err = err2
			}
			close(c.eofReceived)
		case <-c.eofReceived:
		}
		if c.amqpConn != nil {
			if err2 := c.amqpConn.Close(); err2 != nil && err == nil {
				err = err2
			}
		}
		if err != nil {
			err = &net.OpError{
				Op:     "close",
				Net:    c.remoteAddr.Network(),
				Addr:   c.remoteAddr,
				Source: c.localAddr,
				Err:    err,
			}
		}
		close(c.closed)
		return err // leave rChan and wChan stuck
	case <-c.closed:
		return net.ErrClosed
	}
}

// LocalAddr implements net.Conn.LocalAddr
func (c *conn) LocalAddr() net.Addr {
	return c.localAddr
}

// Read implements net.Conn.Read.
func (c *conn) Read(b []byte) (n int, err error) {
	switch {
	case c.closed.isClosed():
		return 0, net.ErrClosed
	case c.eofReceived.isClosed():
		return 0, io.EOF
	case c.rDeadline.Done().isClosed():
		return 0, os.ErrDeadlineExceeded
	case len(b) == 0:
		return 0, nil
	}

	select {
	case c.rChan <- struct{}{}:
		if len(c.rBuf) == 0 {
			select {
			case d, ok := <-c.dChan:
				if !ok {
					c.inChan.Close() // ignore further errors
					close(c.eofReceived)
					return 0, io.ErrUnexpectedEOF // leave rChan stuck
				}
				ackErr := d.Ack(false)
				if len(d.Body) == 0 {
					defer close(c.eofReceived)
					if err := c.inChan.Close(); err != nil {
						return 0, &net.OpError{
							Op:     "read",
							Net:    c.remoteAddr.Network(),
							Addr:   c.remoteAddr,
							Source: c.localAddr,
							Err:    fmt.Errorf("close channel on EOF: %w", err),
						} // leave rChan stuck
					}
					if ackErr != nil {
						return 0, &net.OpError{
							Op:     "read",
							Net:    c.remoteAddr.Network(),
							Addr:   c.remoteAddr,
							Source: c.localAddr,
							Err:    fmt.Errorf("ack delivery on EOF: %w", ackErr),
						} // leave rChan stuck
					}
					return 0, io.EOF // leave rChan stuck
				}
				if ackErr != nil {
					<-c.rChan
					return 0, &net.OpError{
						Op:     "read",
						Net:    c.remoteAddr.Network(),
						Addr:   c.remoteAddr,
						Source: c.localAddr,
						Err:    fmt.Errorf("ack delivery: %w", ackErr),
					}
				}
				c.rBuf = d.Body
			case <-c.rDeadline.Done():
				<-c.rChan
				return 0, os.ErrDeadlineExceeded
			}
		}
		n := copy(b, c.rBuf)
		if n == len(c.rBuf) {
			c.rBuf = nil
		} else {
			c.rBuf = c.rBuf[n:]
		}
		<-c.rChan
		return n, nil
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.eofReceived:
		return 0, io.EOF
	case <-c.rDeadline.Done():
		return 0, os.ErrDeadlineExceeded
	}
}

// RemoteAddr implements net.Conn.RemoteAddr.
func (c *conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// SetDeadline implements net.Conn.SetDeadline.
func (c *conn) SetDeadline(t time.Time) error {
	switch {
	case c.closed.isClosed():
		return net.ErrClosed
	case c.eofReceived.isClosed():
		return io.EOF
	}
	c.rDeadline.Set(t)
	c.wDeadline.Set(t)
	return nil
}

// SetReadDeadline implements net.Conn.SetReadDeadline.
func (c *conn) SetReadDeadline(t time.Time) error {
	switch {
	case c.closed.isClosed():
		return net.ErrClosed
	case c.eofReceived.isClosed():
		return io.EOF
	}
	c.rDeadline.Set(t)
	return nil
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline.
func (c *conn) SetWriteDeadline(t time.Time) error {
	if c.closed.isClosed() {
		return net.ErrClosed
	}
	c.wDeadline.Set(t)
	return nil
}

// Write implements net.Conn.Write.
func (c *conn) Write(b []byte) (n int, err error) {
	switch {
	case c.closed.isClosed():
		return 0, net.ErrClosed
	case c.wDeadline.Done().isClosed():
		return 0, os.ErrDeadlineExceeded
	case len(b) == 0:
		return 0, nil
	}

	select {
	case c.wChan <- struct{}{}:
		errChan := make(chan error, 1)
		go func() {
			defer close(errChan)
			err := c.outChan.Publish("", c.remoteAddr.values.Get("remote_queue"),
				true, false, amqp.Publishing{
					DeliveryMode: amqp.Transient,
					Body:         b,
				})
			if err != nil {
				errChan <- err
			}
		}()
		select {
		case err := <-errChan:
			if err != nil {
				amqpErr, ok := err.(*amqp.Error)
				if !ok || !amqpErr.Recover {
					// try closing the connection, ignore further errors.
					select {
					case c.rChan <- struct{}{}:
						c.inChan.Close()
						close(c.eofReceived)
						c.outChan.Close()
						if c.amqpConn != nil {
							c.amqpConn.Close()
						}
						close(c.closed)
					case <-c.eofReceived:
						c.outChan.Close()
						if c.amqpConn != nil {
							c.amqpConn.Close()
						}
						close(c.closed)
					case <-c.wDeadline.Done():
						<-c.wChan
						return 0, os.ErrDeadlineExceeded
					}
					return 0, &net.OpError{
						Op:     "write",
						Net:    c.remoteAddr.Network(),
						Addr:   c.remoteAddr,
						Source: c.localAddr,
						Err: fmt.Errorf(
							"connection closed due to unrecoverable Publish error: %w", err),
					} // leave rChan and wChan stuck
				}
			}
			<-c.wChan
			if err != nil {
				return 0, &net.OpError{
					Op:     "write",
					Net:    c.remoteAddr.Network(),
					Addr:   c.remoteAddr,
					Source: c.localAddr,
					Err:    tempError{err},
				}
			}
			return len(b), nil
		case <-c.wDeadline.Done():
			// Unfortunately, the AMQP package has no facility to cancel an ongoing
			// publishing, so we simply abandon it.
			<-c.wChan
			return 0, os.ErrDeadlineExceeded
		}
	case <-c.closed:
		return 0, net.ErrClosed
	case <-c.wDeadline.Done():
		return 0, os.ErrDeadlineExceeded
	}
}

// Dial establishes a connection to an AMQP stream server.
// The URL should be a standard amqp(s) URL, which, in addition, must have
// a server_queue parameter set to the server control
// queue name.
// If tlsConfig is nil but a secure connection was requested, an empty
// config with the server name taken from the URL will be used.
//
// Dial is thus a combination of Connect, followed by Connection.Dial. It is a
// useful shortcut if there is only a single AMQP client stream.
func Dial(
	ctx context.Context, urlString string, option ...Option,
) (nc net.Conn, err error) {
	addr, err := newAddr(urlString)
	if err != nil {
		return nil, err
	}
	serverQueueName, err := addr.serverQueueName()
	if err != nil {
		return nil, err
	}
	c, err := Connect(ctx, urlString, option...)
	if err != nil {
		return nil, err
	}
	result, err := c.Dial(ctx, serverQueueName)
	if err != nil {
		return nil, err
	}
	underlying := result.(*conn)
	underlying.amqpConn = c.amqpConn
	return result, nil
}
