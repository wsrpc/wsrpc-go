package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

func bad(p *wsrpc.Peer) {
	value, _ := p.Call[int](context.Background(), "echo", nil)
	var wrong string = value
	_ = wrong
}
