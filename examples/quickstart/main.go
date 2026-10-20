package main

import (
	"context"
	"log"
	"net/http"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

type EchoService struct{ Prefix string }
type EchoParams struct {
	Text string `json:"text"`
}
type EchoResult struct {
	Text string `json:"text"`
}

var (
	Echo     = wsrpc.Define[EchoParams, EchoResult]("echo")
	Callback = wsrpc.Define[Value, int]("callback")
	Double   = wsrpc.Define[Value, int]("double")
)

type Value struct {
	Value int `json:"value"`
}

func (s *EchoService) Echo(_ context.Context, p EchoParams) (EchoResult, error) {
	return EchoResult{Text: s.Prefix + p.Text}, nil
}

func main() {
	routes := new(wsrpc.Router)
	service := &EchoService{Prefix: "Hello, "}
	if err := Echo.Handle(routes, service.Echo); err != nil {
		log.Fatal(err)
	}
	if err := Callback.Handle(routes, func(ctx context.Context, p Value) (int, error) {
		peer, _ := wsrpc.FromContext(ctx)
		return Double.Call(ctx, peer, p)
	}); err != nil {
		log.Fatal(err)
	}
	rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes}})
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws/", rpc)
	log.Fatal(http.ListenAndServe("127.0.0.1:8000", mux))
}
