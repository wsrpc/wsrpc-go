package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/internal/demo"
	"github.com/wsrpc/wsrpc-go/ws"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:8000/ws/", "RPC endpoint")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	routes := new(wsrpc.Router)
	if err := demo.Double.Handle(routes, func(_ context.Context, p demo.Value) (int, error) { return p.Value * 2, nil }); err != nil {
		log.Fatal(err)
	}
	client, err := ws.Dial(ctx, *url, wsrpc.Options{Routes: routes})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close(); client.Wait() }()
	// The legacy echo route accepts arbitrary JSON; select a typed view here.
	echo, err := client.Call[struct {
		Text  string `json:"text"`
		Value int    `json:"value"`
	}](ctx, demo.Echo.Name(), map[string]any{"text": "Привет 🐍", "value": 0})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Echo: %s %d\n", echo.Text, echo.Value)
	result, err := demo.Callback.Bind(client).Call(ctx, demo.Value{Value: 21})
	if err != nil || result != 42 {
		log.Fatalf("callback mismatch: %d %v", result, err)
	}
	fmt.Println("Callback:", result)
}
