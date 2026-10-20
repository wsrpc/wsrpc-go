// Run go run ./examples/objects, then in another terminal:
// go run ./examples/objects -url 'ws://127.0.0.1:8000/ws/?name=alice'
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

func main() {
	url := flag.String("url", "", "WebSocket URL; omit to run the server")
	listen := flag.String("listen", "127.0.0.1:8000", "server listen address")
	flag.Parse()
	if *url != "" {
		if err := runClient(*url); err != nil {
			log.Fatal(err)
		}
		return
	}
	rpc, err := newServer(new(Audit))
	if err != nil {
		log.Fatal(err)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ws://%s/ws/\n", listener.Addr())
	mux := http.NewServeMux()
	mux.Handle("/ws/", rpc)
	log.Fatal(http.Serve(listener, mux))
}

func runClient(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = clientKey.With(ctx, "local CLI")
	routes := new(wsrpc.Router)
	if err := Double.Handle(routes, func(ctx context.Context, p Value) (int, error) {
		label, _ := clientKey.Get(ctx)
		_, hasServerUser := principalKey.Get(ctx)
		call, _ := wsrpc.Current(ctx)
		fmt.Printf("client callback: local=%q server_user_present=%t method=%s\n", label, hasServerUser, call.Method)
		return p.Value * 2, nil
	}); err != nil {
		return err
	}
	peer, err := ws.Dial(ctx, url, wsrpc.Options{Routes: routes})
	if err != nil {
		return err
	}
	defer func() { _ = peer.Close(); peer.Wait() }()
	// Bind once: argument and result types are retained without type assertions.
	init, add := Init.Bind(peer), Add.Bind(peer)
	initial, err := init.Call(ctx, Value{Value: 10})
	if err != nil {
		return err
	}
	fmt.Printf("Init: %d user=%q connection=%s\n", initial.Value, initial.User, initial.Connection)
	for _, delta := range []int{2, 3} {
		next, err := add.Call(ctx, Value{Value: delta})
		if err != nil {
			return err
		}
		fmt.Printf("Add(%d): %d user=%q connection=%s\n", delta, next.Value, next.User, next.Connection)
	}
	return nil
}
