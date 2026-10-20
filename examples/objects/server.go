package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

// Audit is a ready dependency shared by the connection-scoped counters.
type Audit struct{ mu sync.Mutex }

func (a *Audit) Write(format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fmt.Printf(format+"\n", args...)
}

type Counter struct {
	mu    sync.Mutex
	value int
	peer  *wsrpc.Peer
	audit *Audit
}

func (c *Counter) Init(ctx context.Context, p Value) (Snapshot, error) {
	return c.update(ctx, p.Value, true)
}

func (c *Counter) Add(ctx context.Context, p Value) (Snapshot, error) {
	// The incoming handler context selects the caller for a typed reverse call.
	peer, ok := wsrpc.FromContext(ctx)
	if !ok {
		return Snapshot{}, errors.New("missing calling peer")
	}
	doubled, err := Double.Call(ctx, peer, p)
	if err != nil {
		return Snapshot{}, err
	}
	c.audit.Write("callback: peer=%s double(%d)=%d", peer.ID, p.Value, doubled)
	return c.update(ctx, p.Value, false)
}

func (c *Counter) update(ctx context.Context, value int, reset bool) (Snapshot, error) {
	// Values set in HTTP authorization survive the upgrade, factory and dispatch.
	user, hasUser := principalKey.Get(ctx)
	call, hasCall := wsrpc.Current(ctx)
	if !hasUser || !hasCall || call.Peer != c.peer {
		return Snapshot{}, errors.New("missing connection/invocation context")
	}
	c.mu.Lock()
	if reset {
		c.value = value
	} else {
		c.value += value
	}
	result := Snapshot{Value: c.value, User: user.Name, Connection: call.Peer.ID}
	c.mu.Unlock()
	c.audit.Write("handler: user=%q peer=%s call=%d method=%s value=%d", user.Name, call.Peer.ID, call.ID, call.Method, result.Value)
	return result, nil
}

func newServer(audit *Audit) (*ws.Server, error) {
	return ws.NewServer(ws.ServerOptions{
		Authorize: func(r *http.Request) (context.Context, error) {
			// Display identity for this demo, not an authentication mechanism.
			name := r.URL.Query().Get("name")
			if name == "" {
				name = "guest"
			}
			return principalKey.With(r.Context(), Principal{Name: name}), nil
		},
		Peer: wsrpc.Options{Factory: func(peer *wsrpc.Peer, routes *wsrpc.Router) (func(), error) {
			user, _ := principalKey.Get(peer.Context())
			audit.Write("factory: user=%q peer=%s", user.Name, peer.ID)
			counter := &Counter{peer: peer, audit: audit}
			if err := Init.Handle(routes, counter.Init); err != nil {
				return nil, err
			}
			if err := Add.Handle(routes, counter.Add); err != nil {
				return nil, err
			}
			return func() { audit.Write("cleanup: user=%q peer=%s", user.Name, peer.ID) }, nil
		}},
	})
}
