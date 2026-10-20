package invalid

import (
	"context"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

func bad() {
	key := wsrpc.NewContextKey[int]("user ID")
	_ = key.With(context.Background(), "wrong value")
}
