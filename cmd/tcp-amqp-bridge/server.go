package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/TheCount/amqp-stream/stream"
)

// runServer creates an AMQP streaming server according to the specified
// serverURL and forwards incoming connections to the specified tcpServerAddr.
func runServer(tcpServerAddr, serverURL string, opts []stream.Option) {
	for {
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
					log.Printf("Accept AMQP fatal error: %s", err)
					break
				}
				log.Printf("Accept AMQP temporary error: %s", err)
				continue
			}
			go runServerConn(tcpServerAddr, conn)
		}
		log.Println("Re-establishing AMQP stream listener")
	}
}

// runServerConn is the goroutine which bridges the given amqpConn to the
// given existing TCP server.
func runServerConn(tcpServerAddr string, amqpConn net.Conn) {
	defer func() {
		if err := amqpConn.Close(); err != nil {
			log.Printf("Close AMQP connection: %s", err)
		}
	}()
	tcpConn, err := net.Dial("tcp", tcpServerAddr)
	if err != nil {
		log.Printf("Dial tcp '%s': %s", tcpServerAddr, err)
		return
	}
	defer func() {
		if err := tcpConn.Close(); err != nil {
			log.Printf("Close TCP connection: %s", err)
		}
	}()
	if err := bridge(tcpConn, amqpConn); err != nil {
		log.Printf("Error %s", err)
	}
}
