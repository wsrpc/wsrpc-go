package wsrpc

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type captureTransport struct {
	frames  chan []byte
	started chan struct{}
	block   bool
	once    sync.Once
}

func (t *captureTransport) Send(ctx context.Context, data []byte, _ bool) error {
	if t.started != nil {
		t.once.Do(func() { close(t.started) })
	}
	if t.block {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case t.frames <- append([]byte(nil), data...):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (*captureTransport) Close() error { return nil }
func captured(t *testing.T, transport *captureTransport) map[string]json.RawMessage {
	t.Helper()
	select {
	case raw := <-transport.frames:
		var frame map[string]json.RawMessage
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		return frame
	case <-time.After(time.Second):
		t.Fatal("frame timeout")
		return nil
	}
}
func testPeer(t *testing.T, options Options) (*Peer, *captureTransport) {
	t.Helper()
	transport := &captureTransport{frames: make(chan []byte, 128)}
	p, err := NewPeer(context.Background(), transport, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close(); p.Wait() })
	return p, transport
}

func TestWireFalsyNullAndNumbers(t *testing.T) {
	routes := new(Router)
	if err := routes.Handle("echo", func(_ context.Context, p any) (any, error) { return p, nil }); err != nil {
		t.Fatal(err)
	}
	peer, transport := testPeer(t, Options{Routes: routes})
	for _, raw := range []string{"false", "0", "\"\"", "[]", "{}", "9007199254740991"} {
		if err := peer.Receive([]byte(`{"id":3,"method":"echo","params":`+raw+`}`), false); err != nil {
			t.Fatal(err)
		}
		frame := captured(t, transport)
		if string(frame["result"]) != raw {
			t.Fatalf("result %s, want %s", frame["result"], raw)
		}
		// Completion may still be deleting the active ID after sending the reply.
		for {
			peer.mu.Lock()
			active := len(peer.active)
			peer.mu.Unlock()
			if active == 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func TestGenericCallIDsAndLegacyErrors(t *testing.T) {
	for _, client := range []bool{true, false} {
		peer, transport := testPeer(t, Options{Client: client})
		result := make(chan int, 1)
		go func() {
			value, err := peer.Call[int](context.Background(), "double", map[string]int{"value": 21})
			if err != nil {
				result <- -1
				return
			}
			result <- value
		}()
		frame := captured(t, transport)
		id := string(frame["id"])
		want := "2"
		if client {
			want = "3"
		}
		if id != want {
			t.Fatalf("id%s want%s", id, want)
		}
		if err := peer.Receive([]byte(`{"id":`+id+`,"result":42}`), false); err != nil {
			t.Fatal(err)
		}
		if got := <-result; got != 42 {
			t.Fatalf("typed result %d", got)
		}
		errorsCh := make(chan error, 1)
		go func() { _, err := peer.Call[any](context.Background(), "reject", nil); errorsCh <- err }()
		frame = captured(t, transport)
		if err := peer.Receive([]byte(`{"id":`+string(frame["id"])+`,"error":"legacy failure"}`), false); err != nil {
			t.Fatal(err)
		}
		var remote *RemoteError
		if !errors.As(<-errorsCh, &remote) || string(remote.Payload) != `"legacy failure"` {
			t.Fatal("lost legacy error payload")
		}
	}
}

func TestDeadlineDuringBlockedWriteDoesNotClosePeer(t *testing.T) {
	transport := &captureTransport{started: make(chan struct{}), block: true}
	peer, err := NewPeer(context.Background(), transport, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close(); peer.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = peer.Call[int](ctx, "blocked", nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v", err)
	}
	if peer.ctx.Err() != nil {
		t.Fatal("one call deadline closed connection")
	}
	peer.mu.Lock()
	pending := len(peer.pending)
	peer.mu.Unlock()
	if pending != 0 {
		t.Fatal("pending call leaked")
	}
}

func TestCloseWakesPendingAndHandlerContexts(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	routes := new(Router)
	if err := routes.Handle("wait", func(ctx context.Context, _ struct{}) (any, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	peer, transport := testPeer(t, Options{Routes: routes})
	if err := peer.Receive([]byte(`{"id":3,"method":"wait"}`), false); err != nil {
		t.Fatal(err)
	}
	<-started
	ended := make(chan error, 1)
	go func() { _, err := peer.Call[int](context.Background(), "outbound", nil); ended <- err }()
	_ = captured(t, transport)
	_ = peer.Close()
	select {
	case err := <-ended:
		if !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call blocked")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("handler not cancelled")
	}
}

func TestExposurePanicAndSerialization(t *testing.T) {
	routes := new(Router)
	if err := routes.Handle("panic", func(context.Context, struct{}) (any, error) { panic("example panic") }); err != nil {
		t.Fatal(err)
	}
	if err := routes.Handle("channel", func(context.Context, struct{}) (chan int, error) { return make(chan int), nil }); err != nil {
		t.Fatal(err)
	}
	if err := routes.Handle("_private", func(context.Context, struct{}) (int, error) { return 1, nil }); err == nil {
		t.Fatal("private route accepted")
	}
	peer, transport := testPeer(t, Options{Routes: routes})
	for i, method := range []string{"hidden", "panic", "channel"} {
		raw, _ := json.Marshal(map[string]any{"id": i + 3, "method": method})
		if err := peer.Receive(raw, false); err != nil {
			t.Fatal(err)
		}
		frame := captured(t, transport)
		var failure Failure
		if err := json.Unmarshal(frame["error"], &failure); err != nil {
			t.Fatal(err)
		}
		want := []string{"MethodNotFound", "PanicError", "SerializationError"}[i]
		if failure.Type != want {
			t.Fatalf("%s != %s", failure.Type, want)
		}
	}
}

func TestValidationEventsBinaryAndCapacity(t *testing.T) {
	for _, raw := range []string{`[]`, `null`, `{"id":0}`, `{"id":-1}`, `{"id":1.5}`, `{"id":9007199254740992}`, `{"id":3,"method":4}`, `{"id":3,"method":null}`, `{"id":3,"method":"x","result":null}`} {
		p, _ := testPeer(t, Options{})
		if p.Receive([]byte(raw), false) == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	eventsCh := make(chan []byte, 1)
	binaryCh := make(chan []byte, 1)
	p, _ := testPeer(t, Options{OnEvent: func(_ context.Context, data json.RawMessage) { eventsCh <- data }, OnBinary: func(ctx context.Context, data []byte) {
		if _, ok := FromContext(ctx); !ok {
			t.Error("missing connection")
		}
		binaryCh <- data
	}})
	if err := p.Receive([]byte(`{"topic":"updated"}`), false); err != nil {
		t.Fatal(err)
	}
	data := []byte{0, 1, 255}
	if err := p.Receive(data, true); err != nil {
		t.Fatal(err)
	}
	data[0] = 99
	if string(<-eventsCh) != `{"topic":"updated"}` || !reflect.DeepEqual(<-binaryCh, []byte{0, 1, 255}) {
		t.Fatal("event/binary corrupted")
	}
	if p.Emit(context.Background(), map[string]any{"id": 1}) == nil {
		t.Fatal("event with id accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Call[int](ctx, "echo", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
