package main

import (
	"io"
	"log"
	"net"
	"sync"
)

// connectConns reads data from src and writes it to dest. Once EOF is reached,
// dest is closed and *destClosed set to true.
func connectConns(wg *sync.WaitGroup, destClosed *bool, dest, src net.Conn) {
	defer wg.Done()
	buf := make([]byte, 16384)
	for {
		nRead, err := src.Read(buf)
		if err != nil {
			if err == io.EOF {
				err = dest.Close()
				*destClosed = true
				if err != nil {
					log.Printf("Close %s -> %s on EOF: %s",
						dest.LocalAddr(), dest.RemoteAddr(), err)
				}
				return
			}
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Printf("Fatal error reading from %s: %s", src.RemoteAddr(), err)
				return
			}
			log.Printf("Temporary error reading from %s: %s", src.RemoteAddr(), err)
			continue
		}
		writeBuf := buf[:nRead]
		for len(writeBuf) > 0 {
			nWritten, err := dest.Write(writeBuf)
			if err == nil {
				break
			}
			nerr, ok := err.(net.Error)
			if !ok || !nerr.Temporary() {
				log.Printf("Fatal error writing to %s: %s", dest.RemoteAddr(), err)
				return
			}
			writeBuf = writeBuf[nWritten:]
			log.Printf("Temporary error writing to %s: %s", dest.RemoteAddr(), err)
		}
	}
}
