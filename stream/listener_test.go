package stream

import (
	"context"
	"sync"
	"testing"
)

// Tests in this package require a running RabbitMQ server.
//
//     docker pull rabbitmq
//     docker run -p 127.0.0.1:5672:5672 \
//       -d --hostname my-rabbit --name some-rabbit rabbitmq:latest

const serverAddr = "amqp://guest:guest@localhost/?server_queue=testserver"

// TestListen tests listening.
func TestListen(t *testing.T) {
	const expectedServerAddr = "amqp://guest@localhost/?server_queue=testserver"
	l, err := Listen(context.Background(), serverAddr, WithInsecure())
	if err != nil {
		t.Fatalf("listen: %s", err)
	}
	if a := l.Addr().String(); a != expectedServerAddr {
		t.Errorf("expected server address '%s', got '%s'", expectedServerAddr, a)
	}
	if err = l.Close(); err != nil {
		t.Errorf("close listener: %s", err)
	}
}

// TestAccept tests accepting a connection.
func TestAccept(t *testing.T) {
	l, err := Listen(context.Background(), serverAddr, WithInsecure())
	if err != nil {
		t.Fatalf("listen: %s", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := l.Accept()
		if err != nil {
			t.Errorf("accept connection: %s", err)
		} else if err := c.Close(); err != nil {
			t.Errorf("close server connection: %s", err)
		}
		if err = l.Close(); err != nil {
			t.Errorf("close server listener: %s", err)
		}
	}()
	c, err := Dial(context.Background(), serverAddr, WithInsecure())
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("close client connection: %s", err)
	}
	wg.Wait()
}
