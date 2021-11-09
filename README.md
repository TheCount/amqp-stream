amqp-stream implements a generic reliable, ordered, streaming client-server protocol over [AMQP](https://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol). The library is compatible with golang's [net.Conn](https://pkg.go.dev/net#Conn) and [net.Listener](https://pkg.go.dev/net#Listener) interfaces, and can be used to bridge TCP connections over AMQP.

The concept is, to put it quite bluntly, odd. Before you go this route, consider alternatives, such as
* plain [TCP](https://en.wikipedia.org/wiki/Transmission_Control_Protocol) (duh!),
* Web based communication protocols, such as [WebSocket](https://en.wikipedia.org/wiki/WebSocket) or [WebRTC](https://en.wikipedia.org/wiki/WebRTC),
* [STUN](https://en.wikipedia.org/wiki/STUN)/[TURN](https://en.wikipedia.org/wiki/Traversal_Using_Relays_around_NAT)-based solutions if you're having NAT/firewall trouble.

That being said, if you do have an AMQP server (such as [RabbitMQ](https://www.rabbitmq.com/)) already set up, and you need to bridge a TCP connection over a NAT for testing/debugging, amqp-stream may be the quickest solution.

## Try it yourself

This software is in the early stages and probably still has some bugs. Currently, you can run the test suite and go from there. In the future, support binaries for easy TCP bridging are planned.

To get going, you'll need an AMQP server. You can quickly install one for testing purposes using [Docker](https://docker.io):

```sh
docker pull rabbitmq
docker run -p 127.0.0.1:5672:5672 -d --hostname my-rabbit --name some-rabbit rabbitmq:latest
```

This will start a containered RabbitMQ server on your computer, with port 5672 exposed to your local loopback 127.0.0.1.

Clone this repo and run the tests with

```sh
go test -v ./stream
```

Then check out the `stream/*_test.go` files for inspiration how to use this package.

## Concept

To understand how this package works, it makes sense to review how TCP works at the OS/application as well as the network levels. The following very simplified description assumes an ideal situation, i. e., no packet loss/duplication or out-of-sequence packets.

A server app creates a socket and binds the socket to an IP address and a port number. It then starts listening on the socket and can accept incoming connections. This all happens at the OS/application level. The server app does not cause any network traffic at this point.

A client app creates a socket and initiates a connection to the server IP and port. It must have obtained IP and port through some out-of-band means, e. g., from a configuration file, or an OS provided list of well-known port numbers. When the client app initiates the connection, the client's OS sends a TCP package with a special flag (SYN) to the server/port. The server's OS then looks for a server app which has bound that IP/port and is listening for connections. It places the client's connection request into the server app's connection request queue. If the server app is accepting the connection, the server's OS sends a package back to the client with flags (SYN, ACK) indicating the server app accepted the connection. (The client's OS now sends a third package (ACK) to completely establish the connection; necessary in TCP to ensure proper sequencing in both directions between the connection endpoints.)

Once the connection is established, both client and server can send and receive data between each other. This can happen completely asynchronously. The distinction between client and server has vanished at this point. (The endpoint also send packages to acknowledge the receipt of packages.)

Any endpoint can close the connection by sending a package (FIN) to the other endpoint. The other endpoint confirms with (FIN, ACK) (in principle, the other endpoint could just send (ACK) and send its own (FIN) much later, but let's ignore this detail). (The initial endpoint now sends a final (ACK) to completely close the connection.)

In AMQP streaming, there are some analogies to these concepts. Here, the server side consumes messages from an incoming connection queue. Clients wishing to establish a connection must know the AMQP server address and the name of that queue. Furthermore, each connection requires two queues (which need to be created as the connection is established), one for each data flow direction.

The connection establishment process is as follows. First, the client connects to the AMQP server, creates a new queue (the *client queue*, intended for data flowing from the server to the client) and starts consuming from it. The client then sends a message on the control queue with the `Reply-To` header set to the client queue name. The server accepts the connection by creating a new queue (the *server queue*, intended for data flowing from the client to the server) and sending a message to the client queue with the `Reply-To` header set to the server queue name. A third message is not necessary to confirm the connection. Now both client and server know their respective queues and can send data at will.

To close one end of the connection, the endpoint sends a message with empty payload. The other endpoint can then send a message with empty payload on its own end.
