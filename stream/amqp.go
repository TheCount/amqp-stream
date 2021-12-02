package stream

import (
	"context"
	"errors"
	"fmt"
	"net"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Connection is an AMQP connection underlying a listener or a client.
type Connection struct {
	// amqpConn is the AMQP connection.
	amqpConn *amqp.Connection

	// addr is the connection address
	addr *addr
}

// Close closes this connections. This will cause any remaining AMQP streams to
// be severed, their Accept, Close, Read, and Write methods returning errors.
func (c *Connection) Close() error {
	if err := c.amqpConn.Close(); err != nil {
		return fmt.Errorf("close AMQP connection: %w", err)
	}
	return nil
}

// Dial establishes an AMQP client stream on this connection.
// The serverQueueName is the server control queue name.
func (c *Connection) Dial(
	ctx context.Context, serverQueueName string,
) (nc net.Conn, err error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	addr := c.addr.setServerQueueName(serverQueueName)
	// set up incoming data queue
	inChan, err := c.amqpConn.Channel()
	if err != nil {
		return nil, &net.OpError{
			Op:   "dial",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("incoming data channel: %w", err),
		}
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
		return nil, &net.OpError{
			Op:   "dial",
			Net:  addr.Network(),
			Addr: addr,
			Err:  fmt.Errorf("declare incoming data queue: %w", err),
		}
	}
	localAddr := addr.local(q.Name)
	deliveryCh, err := inChan.Consume(q.Name, q.Name,
		false, // manual ack
		true,  // exclusive consumer
		false, // no no-local protection, we don't need it by design
		false, // wait for AMQP server confirmation
		nil,   // no special args
	)
	if err != nil {
		return nil, &net.OpError{
			Op:     "dial",
			Net:    addr.Network(),
			Addr:   addr,
			Source: localAddr,
			Err:    fmt.Errorf("consume from incoming data queue: %w", err),
		}
	}
	// set up outgoing data
	outChan, err := c.amqpConn.Channel()
	if err != nil {
		return nil, &net.OpError{
			Op:     "dial",
			Net:    addr.Network(),
			Addr:   addr,
			Source: localAddr,
			Err:    fmt.Errorf("outgoing data channel: %w", err),
		}
	}
	defer func() {
		if err != nil {
			outChan.Close()
		}
	}()
	if err = outChan.Publish("", serverQueueName, true, false, amqp.Publishing{
		DeliveryMode: amqp.Transient,
		ReplyTo:      q.Name,
	}); err != nil {
		return nil, &net.OpError{
			Op:     "dial",
			Net:    addr.Network(),
			Addr:   addr,
			Source: localAddr,
			Err: fmt.Errorf("initiate connection via '%s': %w",
				serverQueueName, err),
		}
	}
	var outQueueName string
	select {
	case d, ok := <-deliveryCh:
		if !ok {
			return nil, &net.OpError{
				Op:     "dial",
				Net:    addr.Network(),
				Addr:   addr,
				Source: localAddr,
				Err:    errors.New("delivery channel closed"),
			}
		}
		if len(d.Body) > 0 {
			d.Reject(false) // nolint: ignore additional errors
			return nil, &net.OpError{
				Op:     "dial",
				Net:    addr.Network(),
				Addr:   addr,
				Source: localAddr,
				Err:    errors.New("handshake completion with non-empty body"),
			}
		}
		outQueueName = d.ReplyTo
		if outQueueName == "" {
			d.Reject(false) // nolint: ignore additional errors
			return nil, &net.OpError{
				Op:     "dial",
				Net:    addr.Network(),
				Addr:   addr,
				Source: localAddr,
				Err:    errors.New("handshake completion with missing reply-to"),
			}
		}
		if err = d.Ack(false); err != nil {
			return nil, &net.OpError{
				Op:     "dial",
				Net:    addr.Network(),
				Addr:   addr,
				Source: localAddr,
				Err:    fmt.Errorf("acknowledge handshake: %w", err),
			}
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	remoteAddr := addr.remote(outQueueName)
	result := &conn{
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
	}
	return result, nil
}

// Listen creates a new listener on the given server queue using this
// connection.
func (c *Connection) Listen(
	ctx context.Context, serverQueueName string,
) (l net.Listener, err error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	addr := c.addr.setServerQueueName(serverQueueName)
	// set up connection request queue.
	lChan, err := c.amqpConn.Channel()
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
		lChan:  lChan,
		dChan:  deliveryCh,
		addr:   addr,
		closed: make(signalChan),
	}, nil
}

// Connect connects to an AMQP server without starting a Listener or dialling
// to establish an AMQP stream yet. This is useful if multiple servers and/or
// clients should use the same AMQP connection.
// The URL should be a standard amqp(s) URL.
func Connect(
	ctx context.Context, serverURL string, option ...Option,
) (*Connection, error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	amqpAddr, err := newAddr(serverURL)
	if err != nil {
		return nil, err
	}
	var options opts
	for _, o := range option {
		if err = o(&options); err != nil {
			return nil, err
		}
	}
	if err = options.Validate(); err != nil {
		return nil, err
	}
	cfg := amqp.Config{
		TLSClientConfig: options.tlsConfig,
		Dial: func(network, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	if options.auth != nil {
		cfg.SASL = []amqp.Authentication{options.auth}
	}
	amqpConn, err := amqp.DialConfig(serverURL, cfg)
	if err != nil {
		return nil, fmt.Errorf("dial AMQP: %w", err)
	}
	return &Connection{
		amqpConn: amqpConn,
		addr:     amqpAddr,
	}, nil
}
