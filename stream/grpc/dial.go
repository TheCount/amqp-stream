package grpc

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/TheCount/amqp-stream/stream"
	"google.golang.org/grpc"
)

// WithContextDialer returns a gRPC dial option which causes a gRPC client
// connection to connect to an AMQP stream server at the specified URL.
// The given tlsConfig can be nil and will be used for the connction to
// the AMQP server.
//
// The actual grpc.Dial call should use an empty target, e. g.:
//
//     grpc.Dial("", WithContextDialer(
//       "amqp://guest:guest@host/?server_queue=somequeue", nil))
func WithContextDialer(
	serverURL string, tlsConfig *tls.Config,
) grpc.DialOption {
	return grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) {
			return stream.Dial(ctx, serverURL, tlsConfig)
		},
	)
}
