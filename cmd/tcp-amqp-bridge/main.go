package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/TheCount/amqp-stream/stream"
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
	downgradeTLS = app.Flag("downgrade-tls",
		"Normally, this program expects the AMQP server to use TLS 1.3 or better. "+
			"With this flag, the legacy version TLS 1.2 is also accepted.").
		Default("false").Bool()
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
	commandWebClient = app.Command("webclient",
		"Bridge HTTP/1.x clients over AMQP to many servers: "+
			"specify server queue in the "+serverQueueHeader+" header.")
	argWebClientAddr = commandWebClient.Arg("listenAddr",
		"address of HTTP/1.x server to create, for clients to connect to").
		Required().String()
	argWebClientURL = commandWebClient.Arg("serverURL",
		"AMQP server URL, without the server_queue parameter, "+
			"e. g., amqps://host.name").
		Required().String()
)

func main() {
	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("Parse command line: %s", err)
	}
	tlsOpt, err := newTLSOpt(*caPath, *certPath, *keyPath, *downgradeTLS)
	if err != nil {
		log.Fatalf("TLS config: %s", err)
	}
	switch cmd {
	default:
		log.Fatalf("Unsupported command: %s", cmd)
	case commandServer.FullCommand():
		runServer(*argServerAddr, *argServerURL, tlsOpt)
	case commandClient.FullCommand():
		runClient(*argClientAddr, *argClientURL, tlsOpt)
	case commandWebClient.FullCommand():
		runWebClient(*argWebClientAddr, *argWebClientURL, tlsOpt)
	}
}

// newTLSOpt creates a new TLS configuration option from the given paths.
// If all three paths are empty, stream.WithInsecure() is returned instead.
// If caPath is empty, the host's root CA set is used.
func newTLSOpt(
	caPath, certPath, keyPath string, downgradeTLS bool,
) (stream.Option, error) {
	if caPath == "" && certPath == "" && keyPath == "" {
		return stream.WithInsecure(), nil
	}
	if (certPath == "" && keyPath != "") || (certPath != "" && keyPath == "") {
		return nil,
			errors.New("certificate and key path must be specified simultaneously")
	}

	result := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}
	if downgradeTLS {
		result.MinVersion = tls.VersionTLS12
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

	return stream.WithTLSConfig(result), nil
}
