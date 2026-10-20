package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

func TestObjectsKeepStateAndLocalConnectionContexts(t *testing.T) {
	rpc, err := newServer(new(Audit))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rpc.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		server.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connect := func(name string) *wsrpc.Peer {
		t.Helper()
		routes := new(wsrpc.Router)
		if err := Double.Handle(routes, func(ctx context.Context, p Value) (int, error) {
			label, ok := clientKey.Get(ctx)
			_, hasServerIdentity := principalKey.Get(ctx)
			call, hasCall := wsrpc.Current(ctx)
			if !ok || label != name || hasServerIdentity || !hasCall || call.Method != Double.Name() || ctx.Err() != nil {
				return 0, errors.New("callback lost its local context or received the remote context")
			}
			return p.Value * 2, nil
		}); err != nil {
			t.Fatal(err)
		}
		handshake, handshakeCancel := context.WithCancel(clientKey.With(ctx, name))
		peer, err := ws.Dial(handshake, "ws"+strings.TrimPrefix(server.URL, "http")+"/?name="+name, wsrpc.Options{Routes: routes})
		handshakeCancel() // HTTP/handshake lifetime must not become connection lifetime.
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = peer.Close(); peer.Wait() })
		return peer
	}
	first := connect("alice")
	second := connect("bob")
	firstSnapshot, err := Init.Call(ctx, first, Value{Value: 10})
	if err != nil || firstSnapshot.Value != 10 || firstSnapshot.User != "alice" {
		t.Fatalf("init: %+v, %v", firstSnapshot, err)
	}
	add := Add.Bind(first)
	for i, delta := range []int{2, 3} {
		snapshot, err := add.Call(ctx, Value{Value: delta})
		if err != nil || snapshot.Value != []int{12, 15}[i] || snapshot.Connection != firstSnapshot.Connection || snapshot.User != "alice" {
			t.Fatalf("state/context changed across calls: %+v, %v", snapshot, err)
		}
	}
	secondSnapshot, err := Add.Call(ctx, second, Value{Value: 1})
	if err != nil || secondSnapshot.Value != 1 || secondSnapshot.User != "bob" || secondSnapshot.Connection == firstSnapshot.Connection {
		t.Fatalf("connections share state or identity: %+v, %v", secondSnapshot, err)
	}
	var wg sync.WaitGroup
	for range 24 {
		wg.Go(func() {
			if _, err := add.Call(ctx, Value{Value: 1}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	snapshot, err := add.Call(ctx, Value{})
	if err != nil || snapshot.Value != 39 {
		t.Fatalf("concurrent updates lost state: %+v, %v", snapshot, err)
	}
	_ = first.Close()
	first.Wait()
	snapshot, err = Add.Call(ctx, second, Value{Value: 1})
	if err != nil || snapshot.Value != 2 {
		t.Fatalf("closing one object affected another: %+v, %v", snapshot, err)
	}
	third := connect("carol")
	snapshot, err = Add.Call(ctx, third, Value{Value: 1})
	if err != nil || snapshot.Value != 1 || snapshot.User != "carol" || snapshot.Connection == firstSnapshot.Connection {
		t.Fatalf("new connection reused an old object: %+v, %v", snapshot, err)
	}
}
