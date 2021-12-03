package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/TheCount/amqp-stream/stream"
)

// runClient creates a TCP server at the specified address and forwards
// incoming connections to the specified AMQP stream server.
func runClient(tcpServerAddr, serverURL string, opts []stream.Option) {
	l, err := net.Listen("tcp", tcpServerAddr)
	if err != nil {
		log.Fatalf("Listen through TCP: %s", err)
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		amqpConn, err := stream.Connect(ctx, serverURL, opts...)
		cancel()
		if err != nil {
			log.Fatalf("Connect to AMQP: %s", err)
		}
		serverQueueName, err := amqpConn.Addr().ServerQueueName()
		if err != nil {
			log.Fatal(err)
		}
		for !amqpConn.IsClosed() {
			conn, err := l.Accept()
			if err != nil {
				nerr, ok := err.(net.Error)
				if !ok || !nerr.Temporary() {
					log.Fatalf("Accept TCP fatal error: %s", err)
				}
				log.Printf("Accept TCP temporary error: %s", err)
				continue
			}
			go runClientConn(conn, amqpConn, serverQueueName)
		}
		log.Println("Re-establishing AMQP connection")
	}
}

// runClientConn is the goroutine which bridges the given TCP connection
// to the given AMQP stream server.
func runClientConn(
	tcpConn net.Conn, amqpConn *stream.Connection, serverQueueName string,
) {
	var amqpSpec, tcpSpec connSpec
	amqpSpec.src = tcpConn
	tcpSpec.dest = tcpConn
	defer func() {
		if !tcpSpec.destClosed {
			if err := tcpConn.Close(); err != nil {
				log.Printf("Close TCP connection: %s", err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	amqpStreamConn, err := amqpConn.Dial(ctx, serverQueueName)
	cancel()
	if err != nil {
		log.Printf("Dial AMQP: %s", err)
		return
	}
	amqpSpec.dest = amqpStreamConn
	tcpSpec.src = amqpStreamConn
	defer func() {
		if !amqpSpec.destClosed {
			if err := amqpStreamConn.Close(); err != nil {
				log.Printf("Close AMQP stream: %s", err)
			}
		}
	}()
	var wg sync.WaitGroup
	wg.Add(2)
	go connectConns(&wg, &amqpSpec)
	go connectConns(&wg, &tcpSpec)
	wg.Wait()
}
