package main

import (
	"io"
	"log"
	"net"
	"sync"
)

// flusher is implemented by types with the Flush method.
// We need it only in the awkward situation where a call to Close does not
// flush.
type flusher interface {
	// Flush flushes remaining data to the underlying writer.
	Flush() error
}

// connSpec describes a destination and a source endpoint.
type connSpec struct {
	// destClosed indicates whether destCloser.Close has been called. It is safe
	// to read once connectConns has returned.
	destClosed bool

	// destCloser closes the destination connection.
	// Can be nil if dest implements net.Conn.
	destCloser io.Closer

	// destFlusher flushes remaining data to the destination connection before
	// closing it.
	// Can be nil if a call to close already flushes.
	destFlusher flusher

	// dest is the destination endpoint.
	dest io.Writer

	// destLocalAddr and destRemoteAddr are the destination addresses.
	// Can be nil if dest implements net.Conn.
	destLocalAddr, destRemoteAddr net.Addr

	// src is the source endpoint.
	src io.Reader

	// srcAddr is the source address. Can be nil if src implements net.Conn.
	srcAddr net.Addr
}

// connectConns reads data from cs.src and writes it to cs.dest. Once EOF is
// reached, dest is closed (after flushing, if necessary) and cs.destClosed set
// to true.
func connectConns(wg *sync.WaitGroup, cs *connSpec) {
	defer wg.Done()
	if cs.destCloser == nil {
		cs.destCloser = cs.dest.(net.Conn)
	}
	if cs.destLocalAddr == nil {
		cs.destLocalAddr = cs.dest.(net.Conn).LocalAddr()
	}
	if cs.destRemoteAddr == nil {
		cs.destRemoteAddr = cs.dest.(net.Conn).RemoteAddr()
	}
	if cs.srcAddr == nil {
		cs.srcAddr = cs.src.(net.Conn).RemoteAddr()
	}
	buf := make([]byte, 16384)
	for {
		nRead, err := cs.src.Read(buf)
		if err != nil {
			if err == io.EOF {
				if cs.destFlusher != nil {
					if err = cs.destFlusher.Flush(); err != nil {
						log.Printf("Flush %s -> %s on EOF: %s",
							cs.destLocalAddr, cs.destRemoteAddr, err)
					}
				}
				err = cs.destCloser.Close()
				cs.destClosed = true
				if err != nil {
					log.Printf("Close %s -> %s on EOF: %s",
						cs.destLocalAddr, cs.destRemoteAddr, err)
				}
				return
			}
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Printf("Fatal error reading from %s: %s", cs.srcAddr, err)
				return
			}
			log.Printf("Temporary error reading from %s: %s", cs.srcAddr, err)
			continue
		}
		writeBuf := buf[:nRead]
		for len(writeBuf) > 0 {
			nWritten, err := cs.dest.Write(writeBuf)
			if err == nil {
				break
			}
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Printf("Fatal error writing to %s: %s", cs.destRemoteAddr, err)
				return
			}
			writeBuf = writeBuf[nWritten:]
			log.Printf("Temporary error writing to %s: %s", cs.destRemoteAddr, err)
		}
	}
}
