package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

type Args struct{ Value int }

func bad(p *wsrpc.Peer) {
	_, _ = p.Method[Args, int]("counter.add").Call(context.Background(), "wrong argument")
}
