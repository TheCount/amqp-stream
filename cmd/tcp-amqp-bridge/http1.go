package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/TheCount/amqp-stream/stream"
)

const (
	// serverQueueHeader is the HTTP header indicating which AMQP server queue
	// to use.
	serverQueueHeader = "X-AMQP-Server-Queue"
)

// runWebClient creates an HTTP/1.x server at the specified address and
// forwards incoming connections to the AMQP stream server specified in the
// X-AMQP-Server-Queue header in the first request.
func runWebClient(tcpServerAddr, serverURL string, opts ...stream.Option) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	amqpConn, err := stream.Connect(ctx, serverURL, opts...)
	cancel()
	if err != nil {
		log.Fatalf("Connect to AMQP: %s", err)
	}
	defer func() {
		if err := amqpConn.Close(); err != nil {
			log.Fatalf("Close AMQP connection: %s", err)
		}
	}()
	err = http.ListenAndServe(tcpServerAddr, http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		serverQueue := r.Header.Get(serverQueueHeader)
		if serverQueue == "" {
			http.Error(w, serverQueueHeader+" missing", http.StatusBadRequest)
			return
		}
		amqpStreamConn, err := amqpConn.Dial(r.Context(), serverQueue)
		if err != nil {
			http.Error(w, "Dial AMQP stream: "+err.Error(),
				http.StatusServiceUnavailable)
			return
		}
		var amqpSpec, tcpSpec connSpec
		tcpSpec.src = amqpStreamConn
		amqpSpec.dest = amqpStreamConn
		defer func() {
			if !amqpSpec.destClosed {
				if err := amqpStreamConn.Close(); err != nil {
					log.Printf("Close AMQP stream connection: %s", err)
				}
			}
		}()
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Connection hijacking not supported",
				http.StatusInternalServerError)
			return
		}
		requestBytes, err := getRequestBytes(r)
		if err != nil {
			http.Error(w, "Marshal initial request: "+err.Error(),
				http.StatusInternalServerError)
		}
		tcpConn, rwb, err := hj.Hijack()
		if err != nil {
			http.Error(w, "Hijack: "+err.Error(), http.StatusInternalServerError)
			return
		}
		amqpSpec.src = rwb
		amqpSpec.srcAddr = tcpConn.RemoteAddr()
		tcpSpec.dest = rwb
		tcpSpec.destFlusher = rwb
		tcpSpec.destCloser = tcpConn
		tcpSpec.destLocalAddr = tcpConn.LocalAddr()
		tcpSpec.destRemoteAddr = tcpConn.RemoteAddr()
		defer func() {
			if !tcpSpec.destClosed {
				if err := tcpConn.Close(); err != nil {
					log.Printf("Close TCP connection: %s", err)
				}
			}
		}()
		if err := tcpConn.SetDeadline(time.Time{}); err != nil {
			log.Printf("Set TCP deadline: %s", err)
			return
		}
		if _, err := amqpStreamConn.Write(requestBytes); err != nil {
			log.Printf("Send initial request: %s", err)
			return
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go connectConns(&wg, &amqpSpec)
		go connectConns(&wg, &tcpSpec)
		wg.Wait()
	}))
	if err != nil {
		log.Fatalf("Listen through HTTP/1.x: %s", err)
	}
}

// getRequestBytes marshals the specified request to wire format.
func getRequestBytes(r *http.Request) ([]byte, error) {
	pr, pw := io.Pipe()
	pdone := make(chan struct{})
	var perr error
	go func() {
		defer close(pdone)
		defer func() {
			if cerr := pw.Close(); cerr != nil && perr == nil {
				perr = cerr
			}
		}()
		perr = r.Write(pw)
	}()
	requestBytes, err := io.ReadAll(pr)
	if err != nil {
		return nil, fmt.Errorf("read initial request: %w", err)
	}
	<-pdone
	if perr != nil {
		return nil, fmt.Errorf("write initial request: %w", perr)
	}
	return requestBytes, nil
}
