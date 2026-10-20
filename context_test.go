package wsrpc_test

import (
	"context"
	"testing"

	wsrpc "github.com/wsrpc/wsrpc-go"
)

func TestTypedContextKeysKeepIdentityAndParentLifetime(t *testing.T) {
	first := wsrpc.NewContextKey[int]("same label")
	second := wsrpc.NewContextKey[int]("same label")
	otherType := wsrpc.NewContextKey[string]("same label")
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := first.With(parent, 0)
	if value, present := first.Get(ctx); !present || value != 0 {
		t.Fatal("lost a stored zero value")
	}
	if _, present := first.Get(parent); present {
		t.Fatal("With mutated its parent")
	}
	if _, present := second.Get(ctx); present {
		t.Fatal("same-label keys collided")
	}
	if _, present := otherType.Get(ctx); present {
		t.Fatal("different key types collided")
	}
	child := first.With(ctx, 42)
	if value, _ := first.Get(ctx); value != 0 {
		t.Fatal("shadowing mutated an ancestor")
	}
	if value, _ := first.Get(child); value != 42 {
		t.Fatal("shadowing did not store the new value")
	}
	cancel()
	if child.Err() != context.Canceled {
		t.Fatal("typed values detached cancellation")
	}
}

func TestTypedContextKeyDistinguishesAbsentAndNil(t *testing.T) {
	key := wsrpc.NewContextKey[any]("optional")
	if value, present := key.Get(context.Background()); present || value != nil {
		t.Fatal("absent value is not the zero value")
	}
	if value, present := key.Get(key.With(context.Background(), nil)); !present || value != nil {
		t.Fatal("stored nil was treated as absent")
	}
	pointer := wsrpc.NewContextKey[*int]("pointer")
	if value, present := pointer.Get(pointer.With(context.Background(), nil)); !present || value != nil {
		t.Fatal("stored typed nil was treated as absent")
	}
}
