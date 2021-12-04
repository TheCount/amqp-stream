package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/TheCount/amqp-stream/stream"
)

const (
	// serverQueueHeader is the HTTP header indicating which AMQP server queue
	// to use.
	serverQueueHeader = "X-AMQP-Server-Queue"
)

// wendpoint is the writer part of an endpoint, used by runWebClient to
// assemble an endpoint.
type wendpoint interface {
	io.Writer
	RemoteAddr() net.Addr
}

// http1endpoint is the endpoint used for the TCP side in a web client.
type http1endpoint struct {
	io.Reader
	wendpoint
}

var _ endpoint = http1endpoint{}

// runWebClient creates an HTTP/1.x server at the specified address and
// forwards incoming connections to the AMQP stream server specified in the
// X-AMQP-Server-Queue header in the first request.
func runWebClient(tcpServerAddr, serverURL string, opts []stream.Option) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		amqpConn, err := stream.Connect(ctx, serverURL, opts...)
		cancel()
		if err != nil {
			log.Fatalf("Connect to AMQP: %s", err)
		}
		var server *http.Server
		server = &http.Server{
			Addr: tcpServerAddr,
			Handler: http.HandlerFunc(func(
				w http.ResponseWriter, r *http.Request,
			) {
				if amqpConn.IsClosed() {
					server.Close()
					return
				}
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
				defer func() {
					if err := amqpStreamConn.Close(); err != nil {
						log.Printf("Close AMQP stream connection: %s", err)
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
				defer func() {
					if err := tcpConn.Close(); err != nil {
						log.Printf("Close TCP connection: %s", err)
					}
				}()
				if err := tcpConn.SetDeadline(time.Time{}); err != nil {
					log.Printf("Set TCP deadline: %s", err)
					return
				}
				nBuffered := rwb.Reader.Buffered()
				buffered, err := rwb.Peek(nBuffered)
				if err != nil {
					log.Printf("Unable to retrieve buffered bytes: %s", err)
					return
				}
				if err := bridge(http1endpoint{
					Reader: io.MultiReader(
						bytes.NewReader(requestBytes),
						bytes.NewReader(buffered),
						tcpConn,
					),
					wendpoint: tcpConn,
				}, amqpStreamConn); err != nil {
					log.Printf("Error %s", err)
				}
			}),
		}
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %s", err)
		}
		log.Println("Re-establishing AMQP connection")
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
