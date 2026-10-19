package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

type Args struct{ Value int }

func bad(r *wsrpc.Router) {
	_ = r.Handle[Args, int]("add", func(context.Context, Args) (string, error) { return "wrong result", nil })
}
