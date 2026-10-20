// Run the server with go run ./examples/grpc, then run the client with
// go run ./examples/grpc -url ws://127.0.0.1:8000/ws/.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	pb "github.com/wsrpc/wsrpc-go/examples/grpc/helloworld"
	"github.com/wsrpc/wsrpc-go/ws"
	"github.com/wsrpc/wsrpc-go/wsrpcgrpc"
)

type greeter struct {
	pb.UnimplementedGreeterServer
	prefix string
}

func (g *greeter) SayHello(_ context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: g.prefix + req.Name}, nil
}

func (*greeter) Reverse(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	peer, _ := wsrpc.FromContext(ctx)
	client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
	return client.SayHello(ctx, req)
}

func main() {
	url := flag.String("url", "", "WebSocket server URL; omit to run the server")
	listen := flag.String("listen", "127.0.0.1:8000", "server listen address")
	flag.Parse()

	routes := new(wsrpc.Router)
	service := &greeter{prefix: "Hello from server, "}
	if *url != "" {
		service.prefix = "Hello from client, "
	}
	pb.RegisterGreeterServer(wsrpcgrpc.NewRegistrar(routes), service)

	if *url == "" {
		rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes}})
		if err != nil {
			log.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.Handle("/ws/", rpc)
		log.Printf("listening on ws://%s/ws/", *listen)
		log.Fatal(http.ListenAndServe(*listen, mux))
	}

	if err := call(*url, routes); err != nil {
		log.Fatal(err)
	}
}

func call(url string, routes *wsrpc.Router) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := ws.Dial(ctx, url, wsrpc.Options{Routes: routes})
	if err != nil {
		return err
	}
	defer func() { _ = peer.Close(); peer.Wait() }()
	client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
	req := &pb.HelloRequest{Name: "Go"}
	reply, err := client.SayHello(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println(reply.Message)
	// Server invokes the generated SayHello client on this same connection.
	reply, err = client.Reverse(ctx, req)
	if err != nil {
		return err
	}
	fmt.Println(reply.Message)
	return nil
}
