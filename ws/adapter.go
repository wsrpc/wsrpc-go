// Package ws adapts coder/websocket to the transport-independent WSRPC peer.
package ws

import (
	"context"
	"github.com/coder/websocket"
	wsrpc "github.com/wsrpc/wsrpc-go"
	"time"
)

type transport struct {
	conn         *websocket.Conn
	writeTimeout time.Duration
}

func (t transport) Send(ctx context.Context, data []byte, binary bool) error {
	budget := t.writeTimeout
	if budget == 0 {
		budget = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	kind := websocket.MessageText
	if binary {
		kind = websocket.MessageBinary
	}
	return t.conn.Write(ctx, kind, data)
}
func (t transport) Close() error { return t.conn.CloseNow() }

// Serve reads while handlers run in their own goroutines. Caller owns lifecycle.
func Serve(ctx context.Context, conn *websocket.Conn, options wsrpc.Options) (*wsrpc.Peer, error) {
	conn.SetReadLimit(1 << 20)
	p, err := wsrpc.NewPeer(ctx, transport{conn: conn}, options)
	if err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	go readLoop(ctx, conn, p)
	return p, nil
}

func readLoop(ctx context.Context, conn *websocket.Conn, p *wsrpc.Peer) {
	defer p.Wait()
	defer p.Close()
	for {
		kind, data, readErr := conn.Read(ctx)
		if readErr != nil {
			wasClosed := p.Context().Err() != nil
			_ = p.Close()
			status := websocket.CloseStatus(readErr)
			if !wasClosed && status != websocket.StatusNormalClosure && status != websocket.StatusGoingAway {
				_ = p.OnError.Emit(p.Context(), readErr)
			}
			return
		}
		if receiveErr := p.Receive(data, kind == websocket.MessageBinary); receiveErr != nil {
			_ = p.Close()
			_ = p.OnError.Emit(p.Context(), receiveErr)
			return
		}
	}
}

func Dial(ctx context.Context, url string, options wsrpc.Options) (*wsrpc.Peer, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}
	options.Client = true
	// ctx controls the handshake only. Each Call gets its own deadline.
	return Serve(context.WithoutCancel(ctx), conn, options)
}
