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
func runClient(tcpServerAddr, serverURL string) {
	l, err := net.Listen("tcp", tcpServerAddr)
	if err != nil {
		log.Fatalf("Listen through TCP: %s", err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Fatalf("Accept TCP fatal error: %s", err)
			}
			log.Printf("Accept TCP temporary error: %s", err)
			continue
		}
		go runClientConn(conn, serverURL)
	}
}

// runClientConn is the goroutine which bridges the given TCP connection
// to the given AMQP stream server.
func runClientConn(tcpConn net.Conn, serverURL string) {
	var amqpClosed, tcpClosed bool
	defer func() {
		if !tcpClosed {
			if err := tcpConn.Close(); err != nil {
				log.Printf("Close TCP connection: %s", err)
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	amqpConn, err := stream.Dial(ctx, serverURL, nil)
	cancel()
	if err != nil {
		log.Printf("Dial AMQP: %s", err)
		return
	}
	defer func() {
		if !amqpClosed {
			if err := amqpConn.Close(); err != nil {
				log.Printf("Close AMQP connection: %s", err)
			}
		}
	}()
	var wg sync.WaitGroup
	wg.Add(2)
	go connectConns(&wg, &amqpClosed, amqpConn, tcpConn)
	go connectConns(&wg, &tcpClosed, tcpConn, amqpConn)
	wg.Wait()
}