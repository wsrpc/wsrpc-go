package ws_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

type value struct {
	Value int `json:"value"`
}
type inspected struct {
	Value  int    `json:"value"`
	Peer   string `json:"peer"`
	Method string `json:"method"`
	Call   uint64 `json:"call"`
}
type service struct {
	Dependency string
	Calls      atomic.Int64
}

func (s *service) Inspect(ctx context.Context, args value) (inspected, error) {
	before, ok := wsrpc.Current(ctx)
	if !ok {
		return inspected{}, errors.New("missing invocation")
	}
	s.Calls.Add(1)
	// A helper goroutine receives the context explicitly; shared receiver never
	// stores or swaps a socket pointer during concurrent calls.
	result := make(chan int, 1)
	go func() {
		r, err := before.Peer.Call[int](ctx, "double", args)
		if err != nil {
			result <- -1
			return
		}
		result <- r
	}()
	r := <-result
	after, _ := wsrpc.Current(ctx)
	if before != after || s.Dependency != "database" {
		return inspected{}, errors.New("context or dependency changed")
	}
	return inspected{Value: r, Peer: fmt.Sprintf("%p", before.Peer), Method: before.Method, Call: before.ID}, nil
}

type counter struct {
	mu    sync.Mutex
	value int
	Peer  *wsrpc.Peer
}

func (c *counter) Add(ctx context.Context, args value) (int, error) {
	p, ok := wsrpc.FromContext(ctx)
	if !ok || p != c.Peer {
		return 0, errors.New("wrong route binding")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value += args.Value
	return c.value, nil
}

func TestSharedObjectFactoriesContextAndBidirectionalCalls(t *testing.T) {
	routes := new(wsrpc.Router)
	ready := &service{Dependency: "database"}
	if err := routes.Handle("service.inspect", ready.Inspect); err != nil {
		t.Fatal(err)
	}
	cleaned := make(chan struct{}, 2)
	rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes, Factory: func(p *wsrpc.Peer, r *wsrpc.Router) (func(), error) {
		instance := &counter{Peer: p}
		if err := r.Handle("counter.add", instance.Add); err != nil {
			return nil, err
		}
		return func() { cleaned <- struct{}{} }, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var clients []*wsrpc.Peer
	for _, factor := range []int{2, 3} {
		r := new(wsrpc.Router)
		if err := r.Handle("double", func(_ context.Context, args value) (int, error) { return args.Value * factor, nil }); err != nil {
			t.Fatal(err)
		}
		p, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), wsrpc.Options{Routes: r})
		if err != nil {
			t.Fatal(err)
		}
		clients = append(clients, p)
		defer p.Close()
	}
	type checked struct {
		client   int
		response inspected
		err      error
	}
	results := make(chan checked, 40)
	for i, p := range clients {
		for n := 1; n <= 20; n++ {
			go func() {
				r, err := p.Call[inspected](ctx, "service.inspect", value{n})
				if err == nil && r.Value != n*(i+2) {
					err = fmt.Errorf("callback%d", r.Value)
				}
				results <- checked{i, r, err}
			}()
		}
	}
	identities := map[int]string{}
	for range 40 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if previous := identities[r.client]; previous != "" && previous != r.response.Peer {
			t.Fatal("shared object swapped peer")
		}
		identities[r.client] = r.response.Peer
		if r.response.Method != "service.inspect" || r.response.Call%2 != 1 {
			t.Fatal("lost invocation metadata")
		}
	}
	if identities[0] == identities[1] || ready.Calls.Load() != 40 {
		t.Fatal("missing connection isolation")
	}
	for _, p := range clients {
		got, err := p.Remote("counter").Call[int](ctx, "add", value{2})
		if err != nil || got != 2 {
			t.Fatalf("factory instance shared: %d %v", got, err)
		}
	}
	outcomes := rpc.Broadcast[int](ctx, "double", value{10})
	if len(outcomes) != 2 {
		t.Fatal("missing broadcast outcome")
	}
	seen := map[int]bool{}
	for _, out := range outcomes {
		if out.Error != nil {
			t.Fatal(out.Error)
		}
		seen[out.Value] = true
	}
	if !seen[20] || !seen[30] {
		t.Fatal("broadcast lost individual results")
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-cleaned:
		case <-ctx.Done():
			t.Fatal("route cleanup not called")
		}
	}
	if len(rpc.Clients()) != 0 {
		t.Fatal("closed peer retained")
	}
}

func TestEventHandlerCanMakeReverseCallAndBinaryRoundTrip(t *testing.T) {
	rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{OnEvent: func(ctx context.Context, _ json.RawMessage) {
		p, _ := wsrpc.FromContext(ctx)
		v, err := p.Call[int](ctx, "double", value{21})
		if err == nil {
			_ = p.Emit(ctx, map[string]any{"answer": v})
		}
	}, OnBinary: func(ctx context.Context, data []byte) { p, _ := wsrpc.FromContext(ctx); _ = p.SendRaw(ctx, data, true) }}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := new(wsrpc.Router)
	if err := r.Handle("double", func(_ context.Context, v value) (int, error) { return v.Value * 2, nil }); err != nil {
		t.Fatal(err)
	}
	events := make(chan json.RawMessage, 1)
	bytes := make(chan []byte, 1)
	p, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), wsrpc.Options{Routes: r, OnEvent: func(_ context.Context, v json.RawMessage) { events <- v }, OnBinary: func(_ context.Context, v []byte) { bytes <- v }})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Emit(ctx, map[string]any{"ask": true}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-events:
		if string(got) != `{"answer":42}` {
			t.Fatal(string(got))
		}
	case <-ctx.Done():
		t.Fatal("reader blocked by event callback")
	}
	if err := p.SendRaw(ctx, []byte{0, 1, 255}, true); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-bytes:
		if len(got) != 3 || got[2] != 255 {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal("binary timeout")
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorizationAndUpgradePolicies(t *testing.T) {
	rpc, err := ws.NewServer(ws.ServerOptions{Authorize: func(r *http.Request) (context.Context, error) { return nil, errors.New("denied") }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if p, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), wsrpc.Options{}); err == nil {
		_ = p.Close()
		t.Fatal("unauthorized upgrade accepted")
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal(response.StatusCode)
	}
}
