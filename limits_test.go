package wsrpc

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPendingLimitLateRepliesAndIDExhaustion(t *testing.T) {
	p, transport := testPeer(t, Options{MaxPending: 1, Client: true})
	ctx, cancel := context.WithCancel(context.Background())
	ended := make(chan error, 1)
	go func() { _, err := p.Call[int](ctx, "first", nil); ended <- err }()
	first := captured(t, transport)
	if _, err := p.Call[int](context.Background(), "second", nil); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	cancel()
	if err := <-ended; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := p.Receive([]byte(`{"id":`+string(first["id"])+`,"result":42}`), false); err != nil {
		t.Fatal(err)
	}
	if p.PendingCalls() != 0 {
		t.Fatal("late response revived pending call")
	}
	p.mu.Lock()
	p.serial = maxID
	p.mu.Unlock()
	if _, err := p.Call[int](context.Background(), "overflow", nil); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
}

func TestActiveLimitAndDuplicateIDs(t *testing.T) {
	started := make(chan struct{})
	r := new(Router)
	if err := r.Handle("wait", func(ctx context.Context, _ struct{}) (int, error) { close(started); <-ctx.Done(); return 0, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	p, transport := testPeer(t, Options{Routes: r, MaxConcurrent: 1})
	if err := p.Receive([]byte(`{"id":3,"method":"wait"}`), false); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Receive([]byte(`{"id":3,"method":"wait"}`), false); err == nil {
		t.Fatal("duplicate active id accepted")
	}
	if err := p.Receive([]byte(`{"id":5,"method":"wait"}`), false); err != nil {
		t.Fatal(err)
	}
	frame := captured(t, transport)
	var failure Failure
	if err := json.Unmarshal(frame["error"], &failure); err != nil || failure.Type != "CapacityError" {
		t.Fatalf("%v %v", failure, err)
	}
}

func TestQueueByteLimit(t *testing.T) {
	p, _ := testPeer(t, Options{MaxQueuedBytes: 16})
	if err := p.SendRaw(context.Background(), make([]byte, 17), true); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
}

func TestSignalsReplacementAndCleanupOnce(t *testing.T) {
	var cleaned atomic.Int64
	p, transport := testPeer(t, Options{Factory: func(peer *Peer, r *Router) (func(), error) {
		if _, err := peer.Call[int](context.Background(), "early", nil); !errors.Is(err, ErrNotReady) {
			return nil, errors.New("factory call did not fail fast")
		}
		return func() { cleaned.Add(1) }, nil
	}})
	failures := make(chan error, 4)
	p.OnError.Connect(func(_ context.Context, err error) error {
		failures <- err
		return errors.New("terminal listener error")
	})
	var reached atomic.Int64
	disconnect := p.OnCallStart.Connect(func(context.Context, Invocation) error { panic("broken instrumentation") })
	p.OnCallStart.Connect(func(context.Context, Invocation) error { reached.Add(1); return nil })
	if err := p.Handle("value", func(context.Context, struct{}) (int, error) { return 1, nil }); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle("value", func(context.Context, struct{}) (int, error) { return 2, nil }); err != nil {
		t.Fatal(err)
	}
	if err := p.Receive([]byte(`{"id":3,"method":"value"}`), false); err != nil {
		t.Fatal(err)
	}
	frame := captured(t, transport)
	if string(frame["result"]) != "2" {
		t.Fatal(string(frame["result"]))
	}
	select {
	case <-failures:
	case <-time.After(time.Second):
		t.Fatal("listener panic not reported")
	}
	disconnect()
	if !p.DeleteRoute("value") || p.DeleteRoute("value") {
		t.Fatal("delete did not report existence")
	}
	if err := p.Receive([]byte(`{"id":5,"method":"value"}`), false); err != nil {
		t.Fatal(err)
	}
	if len(captured(t, transport)["error"]) == 0 {
		t.Fatal("deleted method remained accessible")
	}
	if reached.Load() != 2 {
		t.Fatal("broken listener prevented later listener")
	}
	_ = p.Close()
	p.Wait()
	p.Wait()
	if cleaned.Load() != 1 {
		t.Fatal("cleanup not exactly once")
	}
}
