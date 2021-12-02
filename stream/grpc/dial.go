package grpc

import (
	"context"
	"net"

	"github.com/TheCount/amqp-stream/stream"
	"google.golang.org/grpc"
)

// WithContextDialer returns a gRPC dial option which causes a gRPC client
// connection to connect to an AMQP stream server at the specified URL.
//
// The actual grpc.Dial call should use an empty target, e. g.:
//
//     grpc.Dial("", WithContextDialer(
//       "amqp://guest:guest@host/?server_queue=somequeue",
//       stream.WithInsecure()))
func WithContextDialer(
	serverURL string, options ...stream.Option,
) grpc.DialOption {
	return grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return stream.Dial(ctx, serverURL, options...)
		},
	)
}

// WithStreamConnection returns a gRPC dial option which causes a gRPC client
// connection to connect to an AMQP stream server using the specified AMQP
// connection and server control queue name.
//
// The actual grpc.Dial call should use an empty target, e. g.:
//
//     grpc.Dial("", WithStreamConnection(conn, "somequeue"))
func WithStreamConnection(
	conn *stream.Connection, serverQueueName string,
) grpc.DialOption {
	return grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return conn.Dial(ctx, serverQueueName)
		},
	)
}
