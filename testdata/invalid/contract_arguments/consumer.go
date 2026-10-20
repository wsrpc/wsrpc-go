package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

func bad(peer *wsrpc.Peer) {
	add := wsrpc.Define[int, int]("counter.add")
	_, _ = add.Call(context.Background(), peer, "wrong argument")
}
