package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

func bad(peer *wsrpc.Peer) {
	add := wsrpc.Define[int, int]("counter.add")
	result, _ := add.Bind(peer).Call(context.Background(), 1)
	var wrong string = result
	_ = wrong
}
