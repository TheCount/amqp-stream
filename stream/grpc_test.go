package stream

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/examples/helloworld/helloworld"
)

// grpcServer is the grpcServer for testing purposes.
type grpcServer struct {
	helloworld.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer.SayHello.
func (*grpcServer) SayHello(
	ctx context.Context, req *helloworld.HelloRequest,
) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{
		Message: "Hello, " + req.GetName(),
	}, nil
}

// TestGRPC tests gRPC via an AMQP stream
func TestGRPC(t *testing.T) {
	l, err := Listen(context.Background(), serverAddr, nil)
	if err != nil {
		t.Fatalf("Listen: %s", err)
	}
	srv := grpc.NewServer()
	helloworld.RegisterGreeterServer(srv, &grpcServer{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(l); err != nil {
			t.Errorf("Listen: %s", err)
		}
	}()
	cc, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithContextDialer(
		func(ctx context.Context, target string) (net.Conn, error) {
			return Dial(ctx, serverAddr, nil)
		},
	))
	if err != nil {
		t.Fatalf("Dial: %s", err)
	}
	cli := helloworld.NewGreeterClient(cc)
	res, err := cli.SayHello(context.Background(), &helloworld.HelloRequest{
		Name: "AMQP Stream",
	})
	if err != nil {
		t.Fatalf("Say hello: %s", err)
	}
	if res.Message != "Hello, AMQP Stream" {
		t.Errorf("Unexpected gRPC message: %s", res.Message)
	}
	if err := cc.Close(); err != nil {
		t.Errorf("Close client: %s", err)
	}
	srv.GracefulStop()
	wg.Wait()
}
