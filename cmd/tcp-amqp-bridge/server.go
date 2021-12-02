package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/TheCount/amqp-stream/stream"
)

// runServer creates an AMQP streaming server according to the specified
// serverURL and forwards incoming connections to the specified tcpServerAddr.
func runServer(tcpServerAddr, serverURL string, opts ...stream.Option) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	l, err := stream.Listen(ctx, serverURL, opts...)
	cancel()
	if err != nil {
		log.Fatalf("Listen through AMQP: %s", err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Fatalf("Accept AMQP fatal error: %s", err)
			}
			log.Printf("Accept AMQP temporary error: %s", err)
			continue
		}
		go runServerConn(tcpServerAddr, conn)
	}
}

// runServerConn is the goroutine which bridges the given amqpConn to the
// given existing TCP server.
func runServerConn(tcpServerAddr string, amqpConn net.Conn) {
	var amqpSpec, tcpSpec connSpec
	amqpSpec.dest = amqpConn
	tcpSpec.src = amqpConn
	defer func() {
		if !amqpSpec.destClosed {
			if err := amqpConn.Close(); err != nil {
				log.Printf("Close AMQP connection: %s", err)
			}
		}
	}()
	tcpConn, err := net.Dial("tcp", tcpServerAddr)
	if err != nil {
		log.Printf("Dial tcp '%s': %s", tcpServerAddr, err)
		return
	}
	amqpSpec.src = tcpConn
	tcpSpec.dest = tcpConn
	defer func() {
		if !tcpSpec.destClosed {
			if err := tcpConn.Close(); err != nil {
				log.Printf("Close TCP connection: %s", err)
			}
		}
	}()
	var wg sync.WaitGroup
	wg.Add(2)
	go connectConns(&wg, &amqpSpec)
	go connectConns(&wg, &tcpSpec)
	wg.Wait()
}
