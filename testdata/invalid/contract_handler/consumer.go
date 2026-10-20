package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

func bad(routes *wsrpc.Router) {
	add := wsrpc.Define[int, int]("counter.add")
	_ = add.Handle(routes, func(context.Context, string) (int, error) { return 0, nil })
}
