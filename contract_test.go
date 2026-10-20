package wsrpc

import (
	"context"
	"encoding/json"
	"testing"
)

func TestProcedureNamespacesRetainBoundReceiver(t *testing.T) {
	add := Define[int, int]("add")
	namespaced := add.In("counter")
	if add.Name() != "add" || namespaced.Name() != "counter.add" || add.In("").Name() != "add" {
		t.Fatal("namespacing mutated the original contract")
	}
	routes := new(Router)
	value := 10
	if err := namespaced.Handle(routes, func(_ context.Context, delta int) (int, error) {
		value += delta
		return value, nil
	}); err != nil {
		t.Fatal(err)
	}
	peer, transport := testPeer(t, Options{Routes: routes})
	for i, delta := range []int{2, 3} {
		request, _ := json.Marshal(map[string]any{"id": 2*i + 3, "method": namespaced.Name(), "params": delta})
		if err := peer.Receive(request, false); err != nil {
			t.Fatal(err)
		}
		var got int
		if err := json.Unmarshal(captured(t, transport)["result"], &got); err != nil {
			t.Fatal(err)
		}
		if want := []int{12, 15}[i]; got != want {
			t.Fatalf("state = %d, want %d", got, want)
		}
	}
}

func TestProcedureRejectsInvalidStaticNames(t *testing.T) {
	for _, name := range []string{"", "a..b", "_private", "a.then"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("invalid static contract was accepted")
				}
			}()
			Define[int, int](name)
		})
	}
	var zero Procedure[int, int]
	if err := zero.Handle(new(Router), func(context.Context, int) (int, error) { return 0, nil }); err == nil {
		t.Fatal("registered the zero contract")
	}
}
