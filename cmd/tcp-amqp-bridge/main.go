package main

import (
	"log"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

// command line arguments
var (
	app = kingpin.New("tcp-amqp-bridge",
		"bridge between TCP and an AMQP stream, client or server")
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
	switch cmd {
	default:
		log.Fatalf("Unsupported command: %s", cmd)
	case commandServer.FullCommand():
		runServer(*argServerAddr, *argServerURL)
	case commandClient.FullCommand():
		runClient(*argClientAddr, *argClientURL)
	}
}
