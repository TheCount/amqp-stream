package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// command line arguments
var (
	app = kingpin.New("tcp-amqp-bridge",
		"bridge between TCP and an AMQP stream, client or server")
	caPath = app.Flag("ca", "CA certificate file for amqps connection. "+
		"If omitted, the host's preinstalled root CAs will be used.").
		Default("").String()
	certPath = app.Flag("cert", "Client certificate file for amqps connection. "+
		"If omitted, only the server will be authenticated.").
		Default("").String()
	keyPath = app.Flag("key", "Private key file for amqps connection. "+
		"Must be specified alongside --cert.").
		Default("").String()
	commandServer = app.Command("server", "Bridge a TCP server over AMQP")
	argServerAddr = commandServer.Arg("serverAddr",
		"address of existing TCP server, e. g. 127.0.0.1:1234").Required().String()
	argServerURL = commandServer.Arg("serverURL",
		"AMQP server URL, e. g. amqps://host.name/?server_queue=queue_name").
		Required().String()
	commandClient = app.Command("client", "Bridge TCP clients over AMQP")
	argClientAddr = commandClient.Arg("listenAddr",
		"address of TCP server to create, for clients to connect to").
		Required().String()
	argClientURL = commandClient.Arg("serverURL",
		"AMQP server URL, e. g. amqps://host.name/?server_queue=queue_name").
		Required().String()
)

func main() {
	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("Parse command line: %s", err)
	}
	tlsConfig, err := newTLSConfig(*caPath, *certPath, *keyPath)
	if err != nil {
		log.Fatalf("TLS config: %s", err)
	}
	switch cmd {
	default:
		log.Fatalf("Unsupported command: %s", cmd)
	case commandServer.FullCommand():
		runServer(*argServerAddr, *argServerURL, tlsConfig)
	case commandClient.FullCommand():
		runClient(*argClientAddr, *argClientURL, tlsConfig)
	}
}

// newTLSConfig creates a new TLS configuration from the given paths.
// If all three paths are empty, a nil configuration is returned (no TLS desired
// or server verification only). If caPath is empty, the host's root CA set is
// used.
func newTLSConfig(caPath, certPath, keyPath string) (*tls.Config, error) {
	if caPath == "" && certPath == "" && keyPath == "" {
		return nil, nil
	}
	if (certPath == "" && keyPath != "") || (certPath != "" && keyPath == "") {
		return nil,
			errors.New("certificate and key path must be specified simultaneously")
	}

	result := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	if caPath != "" {
		ca, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA from '%s': %w", caPath, err)
		}
		result.RootCAs = x509.NewCertPool()
		if ok := result.RootCAs.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("no CA certificate found in '%s'", caPath)
		}
	}

	if certPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load key pair: %w", err)
		}
		result.Certificates = []tls.Certificate{cert}
	}

	return result, nil
}
