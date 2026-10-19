package ws_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	wsrpc "github.com/wsrpc/wsrpc-go"
	"github.com/wsrpc/wsrpc-go/ws"
)

func TestAuthorizedRequestValuesSurviveUpgrade(t *testing.T) {
	type userKey struct{}
	routes := new(wsrpc.Router)
	if err := routes.Handle("who", func(ctx context.Context, _ struct{}) (string, error) { return ctx.Value(userKey{}).(string), nil }); err != nil {
		t.Fatal(err)
	}
	rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes}, Authorize: func(r *http.Request) (context.Context, error) {
		return context.WithValue(r.Context(), userKey{}, "alice"), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), wsrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	got, err := p.Call[string](ctx, "who", nil)
	if err != nil || got != "alice" {
		t.Fatalf("%q %v", got, err)
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCrossOriginRejectedAndPayloadBounded(t *testing.T) {
	rpc, err := ws.NewServer(ws.ServerOptions{MaxMessageBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}}})
	if err == nil {
		_ = conn.CloseNow()
		t.Fatal("foreign origin accepted")
	}
	p, err := ws.Dial(ctx, url, wsrpc.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Call[any](ctx, "ping", map[string]string{"data": strings.Repeat("x", 100)}); err == nil {
		t.Fatal("oversized payload accepted")
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConnectCallbackHasLiveReaderAndShutdownCancelsHook(t *testing.T) {
	rpc, err := ws.NewServer(ws.ServerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	answer := make(chan int, 1)
	stopped := make(chan struct{})
	rpc.OnConnect.Connect(func(ctx context.Context, p *wsrpc.Peer) error {
		got, err := p.Call[int](ctx, "double", value{21})
		if err != nil {
			return err
		}
		answer <- got
		<-ctx.Done()
		close(stopped)
		return nil
	})
	server := httptest.NewServer(rpc)
	defer server.Close()
	routes := new(wsrpc.Router)
	if err := routes.Handle("double", func(_ context.Context, v value) (int, error) { return v.Value * 2, nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), wsrpc.Options{Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	select {
	case got := <-answer:
		if got != 42 {
			t.Fatal(got)
		}
	case <-ctx.Done():
		t.Fatal("OnConnect reverse call blocked")
	}
	if err := rpc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("shutdown did not join connect listener")
	}
}
