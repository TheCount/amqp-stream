package stream

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

// Tests in this package require a running RabbitMQ server.
//
//     docker pull rabbitmq
//     docker run -p 127.0.0.1:5672:5672 \
//       -d --hostname my-rabbit --name some-rabbit rabbitmq:latest

// TestHello tests sending a simple message between client and server.
func TestHello(t *testing.T) {
	var testmsg = []byte("Hello, world!")
	l, err := Listen(context.Background(), serverAddr, nil)
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
		}
		buf := make([]byte, 1024)
		n, err := c.Read(buf)
		if err != nil {
			t.Errorf("server read: %s", err)
		}
		if !bytes.Equal(buf[:n], testmsg) {
			t.Errorf("server expected test msg '%s', got '%s'", testmsg, buf[:n])
		}
		if _, err = c.Write(testmsg); err != nil {
			t.Errorf("server write: %s", err)
		}
		if err = c.Close(); err != nil {
			t.Errorf("server close: %s", err)
		}
		if err = l.Close(); err != nil {
			t.Errorf("listener close: %s", err)
		}
	}()
	c, err := Dial(context.Background(), serverAddr, nil)
	if err != nil {
		t.Fatalf("dial: %s", err)
	}
	if _, err = c.Write(testmsg); err != nil {
		t.Errorf("client write test msg: %s", err)
	}
	buf := make([]byte, 1024)
	n, err := c.Read(buf)
	if err != nil {
		t.Errorf("client read test msg: %s", err)
	}
	if !bytes.Equal(buf[:n], testmsg) {
		t.Errorf("client expected test msg '%s', got '%s'", testmsg, buf[:n])
	}
	if err = c.Close(); err != nil {
		t.Errorf("client close: %s", err)
	}
	wg.Wait()
}
