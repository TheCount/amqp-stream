package stream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/streadway/amqp"
)

// listener implements a net.Listener for an AMQP stream connection.
// listener must not be copied once in use.
type listener struct {
	// amqpConn is the AMQP client connection for the AMQP stream server.
	amqpConn *amqp.Connection

	// lChan is the AMQP channel for consuming connection requests.
	lChan *amqp.Channel

	// dChan is the AMQP delivery channel.
	dChan <-chan amqp.Delivery

	// addr is the address of this listener.
	addr *addr

	// closeOnce ensures that the close operations are executed only once.
	closeOnce sync.Once

	// closed indicates whether this listener has been closed.
	closed signalChan
}

// Accept implements net.Listener.Accept.
func (l *listener) Accept() (net.Conn, error) {
	if l.closed.isClosed() {
		return nil, net.ErrClosed
	}

	select {
	case d, ok := <-l.dChan:
		if !ok {
			l.Close()
			return nil, &net.OpError{
				Op:   "accept",
				Net:  l.addr.Network(),
				Addr: l.addr,
				Err:  errors.New("delivery channel closed"),
			}
		}
		if len(d.Body) != 0 {
			return nil, &net.OpError{
				Op:   "accept",
				Net:  l.addr.Network(),
				Addr: l.addr,
				Err:  tempError{errors.New("connection request has payload")},
			}
		}
		if d.ReplyTo == "" {
			return nil, &net.OpError{
				Op:   "accept",
				Net:  l.addr.Network(),
				Addr: l.addr,
				Err:  tempError{errors.New("connection request lacks reply-to")},
			}
		}
		// set up server connection channel
		var (
			errChan               = make(chan error, 1)
			inChan, outChan       *amqp.Channel
			localAddr, remoteAddr *addr
			deliveryCh            <-chan amqp.Delivery
		)
		go func() {
			defer close(errChan)
			remoteAddr = l.addr.remote(d.ReplyTo)
			var err error
			inChan, err = l.amqpConn.Channel()
			if err != nil {
				errChan <- &net.OpError{
					Op:   "accept",
					Net:  l.addr.Network(),
					Addr: l.addr,
					Err:  fmt.Errorf("incoming data channel: %w", err),
				}
				return
			}
			defer func() {
				if err != nil {
					inChan.Close()
				}
			}()
			q, err := inChan.QueueDeclare(
				"",    // generate name
				false, // non-durable
				true,  // auto-delete
				true,  // exclusive
				false, // wait for AMQP server confirmation
				nil,   // no special args
			)
			if err != nil {
				errChan <- &net.OpError{
					Op:   "accept",
					Net:  l.addr.Network(),
					Addr: l.addr,
					Err:  fmt.Errorf("declare incoming data queue: %w", err),
				}
				return
			}
			localAddr = l.addr.local(q.Name)
			if deliveryCh, err = inChan.Consume(q.Name, q.Name,
				false, // manual ack
				true,  // exclusive consumer
				false, // no no-local protection, we don't need it by design
				false, // wait for AMQP server confirmation
				nil,   // no special args
			); err != nil {
				errChan <- &net.OpError{
					Op:     "accept",
					Net:    remoteAddr.Network(),
					Addr:   remoteAddr,
					Source: localAddr,
					Err:    fmt.Errorf("consume from incoming data queue: %w", err),
				}
			}
			outChan, err = l.amqpConn.Channel()
			if err != nil {
				errChan <- &net.OpError{
					Op:   "accept",
					Net:  l.addr.Network(),
					Addr: l.addr,
					Err:  fmt.Errorf("outgoing data channel: %w", err),
				}
				return
			}
			defer func() {
				if err != nil {
					outChan.Close()
				}
			}()
			if err = outChan.Publish("", d.ReplyTo, true, false, amqp.Publishing{
				DeliveryMode: amqp.Transient,
				ReplyTo:      q.Name,
			}); err != nil {
				errChan <- &net.OpError{
					Op:     "accept",
					Net:    remoteAddr.Network(),
					Addr:   remoteAddr,
					Source: localAddr,
					Err:    fmt.Errorf("complete handshake: %w", err),
				}
			}
		}()
		select {
		case grErr := <-errChan:
			if grErr != nil {
				return nil, grErr
			}
			return &conn{
				inChan:      inChan,
				outChan:     outChan,
				dChan:       deliveryCh,
				localAddr:   localAddr,
				remoteAddr:  remoteAddr,
				rChan:       make(chan struct{}, 1),
				wChan:       make(chan struct{}, 1),
				rDeadline:   newDeadline(),
				wDeadline:   newDeadline(),
				eofReceived: make(signalChan),
				closed:      make(signalChan),
			}, nil
		case <-l.closed:
			return nil, net.ErrClosed
		}
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Addr implements net.Listener.Addr.
func (l *listener) Addr() net.Addr {
	return l.addr
}

// Close implements net.Listener.Close.
func (l *listener) Close() error {
	err := net.ErrClosed
	l.closeOnce.Do(func() {
		close(l.closed)
		err = l.lChan.Close()
		if err2 := l.amqpConn.Close(); err2 != nil && err == nil {
			err = err2
		}
		if err != nil {
			err = &net.OpError{
				Op:   "close",
				Net:  l.addr.Network(),
				Addr: l.addr,
				Err:  err,
			}
		}
	})
	return err
}

var _ net.Listener = (*listener)(nil)

// Listen creates an AMQP stream server listener.
// The URL should be a standard amqp(s) URL, which, in addition, must have
// a server_queue parameter set to the server control
// queue name.
// If tlsConfig is nil but a secure connection was requested, an empty
// config with the server name taken from the URL will be used.
func Listen(
	ctx context.Context, urlString string, tlsConfig *tls.Config,
) (l net.Listener, err error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	addr, err := newAddr(urlString)
	if err != nil {
		return nil, err
	}
	serverQueueName, err := addr.serverQueueName()
	if err != nil {
		return nil, err
	}
	// establish AMQP connection
	// FIXME: use DialConfig with net.Dialer.DialContext
	var amqpConn *amqp.Connection
	if tlsConfig == nil {
		amqpConn, err = amqp.Dial(urlString)
	} else {
		amqpConn, err = amqp.DialTLS(urlString, tlsConfig)
	}
	if err != nil {
		return nil, &net.OpError{
			Op:   "listen",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("dial '%s': %w", urlString, err),
		}
	}
	defer func() {
		if err != nil {
			amqpConn.Close()
		}
	}()
	// set up connection request queue.
	lChan, err := amqpConn.Channel()
	if err != nil {
		return nil, &net.OpError{
			Op:   "listen",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("connection request channel: %w", err),
		}
	}
	defer func() {
		if err != nil {
			lChan.Close()
		}
	}()
	if _, err = lChan.QueueDeclare(serverQueueName,
		false, // non-durable
		true,  // auto-delete
		true,  // exclusive
		false, // wait for AMQP server confirmation
		nil,   // no special args
	); err != nil {
		return nil, &net.OpError{
			Op:   "listen",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("declare server queue: %w", err),
		}
	}
	deliveryCh, err := lChan.Consume(serverQueueName, serverQueueName,
		false, // manual ack
		true,  // exclusive consumer
		false, // no no-local protection, we don't need it by design
		false, // wait for AMQP server confirmation
		nil,   // no special args
	)
	if err != nil {
		return nil, &net.OpError{
			Op:   "listen",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("consume from connection request queue: %w", err),
		}
	}
	return &listener{
		amqpConn: amqpConn,
		lChan:    lChan,
		dChan:    deliveryCh,
		addr:     addr,
		closed:   make(signalChan),
	}, nil
}
