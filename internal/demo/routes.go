// Package demo contains the runnable examples' routes, also used by interop tests.
package demo

import (
	"context"
	"errors"
	"sync"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

type Value struct {
	Value int `json:"value"`
}
type Counter struct {
	mu    sync.Mutex
	value int
}

func (c *Counter) Init(_ context.Context, p Value) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = p.Value
	return c.value, nil
}
func (c *Counter) Add(_ context.Context, p Value) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += p.Value
	return c.value, nil
}

// EchoService is initialized once with application dependencies and shared.
type EchoService struct{ Label string }

func (s *EchoService) Echo(_ context.Context, p map[string]any) (map[string]any, error) {
	return p, nil
}

func NewServer() (*ws.Server, error) {
	routes := new(wsrpc.Router)
	service := &EchoService{Label: "ready"}
	if err := Echo.Handle(routes, service.Echo); err != nil {
		return nil, err
	}
	reverse := func(ctx context.Context, p Value) (int, error) {
		peer, _ := wsrpc.FromContext(ctx)
		return Double.Call(ctx, peer, p)
	}
	for _, method := range []wsrpc.Procedure[Value, int]{Reverse, Callback} {
		if err := method.Handle(routes, reverse); err != nil {
			return nil, err
		}
	}
	if err := routes.Handle("deferredCallback", func(ctx context.Context, p Value) (int, error) {
		peer, _ := wsrpc.FromContext(ctx)
		return Deferred.Call(ctx, peer, p)
	}); err != nil {
		return nil, err
	}
	if err := routes.Handle("math.add", func(ctx context.Context, p struct {
		A int `json:"a"`
		B int `json:"b"`
	}) (int, error) {
		return p.A + p.B, nil
	}); err != nil {
		return nil, err
	}
	failure := func(context.Context, struct{}) (any, error) { return nil, errors.New("legacy failure") }
	for _, name := range []string{"fail", "failure"} {
		if err := routes.Handle(name, failure); err != nil {
			return nil, err
		}
	}
	return ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes, Factory: func(peer *wsrpc.Peer, r *wsrpc.Router) (func(), error) {
		counter := new(Counter)
		methods := new(wsrpc.Router)
		if err := Init.Handle(methods, counter.Init); err != nil {
			return nil, err
		}
		if err := Add.Handle(methods, counter.Add); err != nil {
			return nil, err
		}
		if err := r.Mount("counter", methods); err != nil {
			return nil, err
		}
		peer.OnBinary.Connect(func(ctx context.Context, data []byte) error {
			ints := make([]int, len(data))
			for i, b := range data {
				ints[i] = int(b)
			}
			return peer.Emit(peer.Context(), map[string]any{"topic": "binary", "data": ints})
		})
		return nil, nil
	}}})
}
