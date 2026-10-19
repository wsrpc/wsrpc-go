// Package wsrpc implements bidirectional WebSocket RPC compatible with Python and TS WSRPC.
package wsrpc

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const maxID = uint64(1<<53 - 1) // IDs must remain exactly representable by JS clients.

var ErrClosed = errors.New("RPC connection closed")
var ErrCapacity = errors.New("RPC capacity exceeded")
var ErrNotReady = errors.New("peer is initializing; use OnConnect for remote calls")

// Transport does not depend on HTTP or a particular WebSocket implementation.
// Send must honor ctx and must not retain data after returning; Close unblocks Send.
type Transport interface {
	Send(ctx context.Context, data []byte, binary bool) error
	Close() error
}

type Failure struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

func (e *Failure) Error() string { return e.Message }

// RemoteError preserves even non-object errors from legacy JavaScript clients.
type RemoteError struct{ Payload json.RawMessage }

func (e *RemoteError) Error() string {
	var v struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Payload, &v) == nil && v.Message != "" {
		return v.Message
	}
	return string(e.Payload)
}

type Options struct {
	Client bool
	Routes *Router
	// Factory runs once for each connection; shared dependencies may be captured.
	Factory        func(*Peer, *Router) (cleanup func(), err error)
	MaxPending     int
	MaxConcurrent  int
	MaxQueuedBytes int
	OnEvent        func(context.Context, json.RawMessage)
	OnBinary       func(context.Context, []byte)
}

type response struct {
	raw json.RawMessage
	err error
}

type outgoing struct {
	ctx    context.Context
	data   []byte
	binary bool
	ack    chan error
}

type Peer struct {
	ID            string
	OnEvent       Signal[json.RawMessage]
	OnBinary      Signal[[]byte]
	OnClose       Signal[*Peer]
	OnError       Signal[error]
	OnCallStart   Signal[Invocation]
	OnCallSuccess Signal[CallSuccess]
	OnCallFail    Signal[CallFailure]
	transport     Transport
	ctx           context.Context
	cancel        context.CancelFunc
	mu            sync.Mutex
	routesMu      sync.RWMutex
	outgoing      chan outgoing
	writerDone    chan struct{}
	closed        bool
	queuedBytes   int
	serial        uint64
	pending       map[uint64]chan response
	active        map[uint64]struct{}
	methods       map[string]handler
	cleanup       func()
	options       Options
	workers       sync.WaitGroup
	jobs          chan struct{}
	finishOnce    sync.Once
	ready         atomic.Bool
}

type CallSuccess struct {
	Invocation
	Result any
}
type CallFailure struct {
	Invocation
	Error error
}

func NewPeer(ctx context.Context, transport Transport, options Options) (*Peer, error) {
	if transport == nil {
		return nil, errors.New("transport is required")
	}
	if options.MaxPending < 0 || options.MaxConcurrent < 0 || options.MaxQueuedBytes < 0 {
		return nil, errors.New("negative capacity")
	}
	if options.MaxPending == 0 {
		options.MaxPending = 1024
	}
	if options.MaxConcurrent == 0 {
		options.MaxConcurrent = 128
	}
	if options.MaxQueuedBytes == 0 {
		options.MaxQueuedBytes = 8 << 20
	}
	connection, cancel := context.WithCancel(ctx)
	p := &Peer{transport: transport, ctx: connection, cancel: cancel, pending: make(map[uint64]chan response), active: make(map[uint64]struct{}), options: options}
	p.ID = rand.Text()
	p.ctx = context.WithValue(connection, peerKey{}, p)
	p.jobs = make(chan struct{}, options.MaxConcurrent)
	if options.Client {
		p.serial = 1
	}
	router := &Router{methods: options.Routes.snapshot()}
	if options.Factory != nil {
		cleanup, err := makeRoutes(options.Factory, p, router)
		if err != nil {
			cancel()
			return nil, err
		}
		p.cleanup = cleanup
	}
	p.methods = router.snapshot()
	if _, exists := p.methods["ping"]; !exists {
		p.methods["ping"] = func(_ context.Context, raw json.RawMessage) (any, error) { return raw, nil }
	}
	p.outgoing = make(chan outgoing, 128)
	p.writerDone = make(chan struct{})
	p.ready.Store(true)
	go p.writeLoop()
	return p, nil
}

// Call is synchronous for its goroutine. The reader continues independently, so
// handlers can call the other endpoint, including nested reverse calls.
// ctx cancels this local wait; the existing wire protocol has no cancellation frame.
func (p *Peer) Call[R any](ctx context.Context, method string, params any) (R, error) {
	var result R
	if !p.ready.Load() {
		return result, ErrNotReady
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !safeName(method) {
		return result, errors.New("invalid method name")
	}
	p.mu.Lock()
	if p.closed || p.ctx.Err() != nil {
		p.mu.Unlock()
		return result, ErrClosed
	}
	if len(p.pending) >= p.options.MaxPending || p.serial > maxID-2 {
		p.mu.Unlock()
		return result, ErrCapacity
	}
	p.serial += 2
	id := p.serial
	ch := make(chan response, 1)
	p.pending[id] = ch
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	if params == nil {
		params = map[string]any{}
	}
	data, err := encode(map[string]any{"id": id, "method": method, "params": params})
	if err != nil {
		return result, err
	}
	// Queueing and waiting honor the caller deadline without cancelling a shared
	// WebSocket write. The bounded writer prevents an unbounded goroutine backlog.
	if err := p.enqueue(outgoing{ctx: ctx, data: data}); err != nil {
		return result, err
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-p.ctx.Done():
		return result, ErrClosed
	case reply := <-ch:
		if reply.err != nil {
			return result, reply.err
		}
		err := decode(reply.raw, &result)
		return result, err
	}
}

func (p *Peer) send(value any) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	return p.SendRaw(p.ctx, data, false)
}

func (p *Peer) SendRaw(ctx context.Context, data []byte, binary bool) error {
	if !p.ready.Load() {
		return ErrNotReady
	}
	ack := make(chan error, 1)
	if err := p.enqueue(outgoing{ctx: ctx, data: append([]byte(nil), data...), binary: binary, ack: ack}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return ErrClosed
	case err := <-ack:
		return err
	}
}

func (p *Peer) enqueue(item outgoing) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if len(item.data) > p.options.MaxQueuedBytes-p.queuedBytes {
		p.mu.Unlock()
		return ErrCapacity
	}
	p.queuedBytes += len(item.data)
	p.mu.Unlock()
	queued := false
	defer func() {
		if !queued {
			p.releaseBytes(len(item.data))
		}
	}()
	select {
	case <-p.ctx.Done():
		return ErrClosed
	case <-item.ctx.Done():
		return item.ctx.Err()
	case p.outgoing <- item:
		queued = true
		return nil
	}
}

func (p *Peer) releaseBytes(size int) { p.mu.Lock(); p.queuedBytes -= size; p.mu.Unlock() }

func (p *Peer) writeLoop() {
	defer close(p.writerDone)
	defer p.Close()
	for {
		select {
		case <-p.ctx.Done():
			return
		case item := <-p.outgoing:
			if item.ctx.Err() != nil {
				p.releaseBytes(len(item.data))
				continue
			}
			err := p.transport.Send(p.ctx, item.data, item.binary)
			p.releaseBytes(len(item.data))
			if item.ack != nil {
				item.ack <- err
			}
			if err != nil {
				_ = p.Close()
				p.notify(err)
				return
			}
		}
	}
}

func (p *Peer) Emit(ctx context.Context, event map[string]any) error {
	if _, exists := event["id"]; exists {
		return errors.New("events cannot contain id")
	}
	data, err := encode(event)
	if err != nil {
		return err
	}
	return p.SendRaw(ctx, data, false)
}

// Receive validates and schedules an incoming call without blocking the reader.
// One transport reader invokes Receive; handlers execute concurrently.
func (p *Peer) Receive(data []byte, binary bool) error {
	if p.ctx.Err() != nil {
		return ErrClosed
	}
	if binary {
		copy := append([]byte(nil), data...)
		return p.event(func() {
			if p.options.OnBinary != nil {
				p.options.OnBinary(p.ctx, copy)
			}
			p.notify(p.OnBinary.Emit(p.ctx, copy))
		})
	}
	var frame map[string]json.RawMessage
	if err := json.Unmarshal(data, &frame); err != nil || frame == nil {
		return errors.New("expected RPC JSON object")
	}
	idRaw, hasID := frame["id"]
	if !hasID {
		copy := append(json.RawMessage(nil), data...)
		return p.event(func() {
			if p.options.OnEvent != nil {
				p.options.OnEvent(p.ctx, copy)
			}
			p.notify(p.OnEvent.Emit(p.ctx, copy))
		})
	}
	var id uint64
	if err := json.Unmarshal(idRaw, &id); err != nil || id == 0 || id > maxID {
		return errors.New("invalid RPC identifier")
	}
	if methodRaw, request := frame["method"]; request {
		var method string
		_, resultPresent := frame["result"]
		_, errorPresent := frame["error"]
		if err := json.Unmarshal(methodRaw, &method); err != nil || string(methodRaw) == "null" || resultPresent || errorPresent {
			return errors.New("invalid RPC request")
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrClosed
		}
		if _, duplicate := p.active[id]; duplicate {
			p.mu.Unlock()
			return errors.New("duplicate active call id")
		}
		if !p.acquireJob() {
			p.mu.Unlock()
			return p.send(map[string]any{"id": id, "error": &Failure{Type: "CapacityError", Message: ErrCapacity.Error()}})
		}
		p.active[id] = struct{}{}
		p.workers.Add(1)
		p.mu.Unlock()
		params := frame["params"]
		if len(params) == 0 || string(params) == "null" {
			params = json.RawMessage("{}")
		}
		go p.dispatch(id, method, params)
		return nil
	}
	p.mu.Lock()
	ch := p.pending[id]
	delete(p.pending, id)
	p.mu.Unlock()
	if ch == nil {
		return nil
	} // Late replies after a local timeout are harmless.
	if failure, failed := frame["error"]; failed {
		ch <- response{err: &RemoteError{Payload: failure}}
		return nil
	}
	raw, present := frame["result"]
	if !present {
		raw = json.RawMessage("null")
	}
	ch <- response{raw: raw}
	return nil
}

func (p *Peer) dispatch(id uint64, method string, params json.RawMessage) {
	defer p.workers.Done()
	defer func() { <-p.jobs }()
	defer func() { p.mu.Lock(); delete(p.active, id); p.mu.Unlock() }()
	ctx := context.WithValue(p.ctx, callKey{}, Invocation{Peer: p, ID: id, Method: method})
	call, _ := Current(ctx)
	p.notify(p.OnCallStart.Emit(ctx, call))
	p.routesMu.RLock()
	fn := p.methods[method]
	p.routesMu.RUnlock()
	result, err := invoke(ctx, fn, params)
	if !safeName(method) {
		result, err = nil, &Failure{Type: "MethodNotFound", Message: "method not exposed"}
	}
	var frame any = map[string]any{"id": id, "result": result}
	if err != nil {
		p.notify(p.OnCallFail.Emit(ctx, CallFailure{Invocation: call, Error: err}))
		failure := &Failure{Type: "Error", Message: fmt.Sprint(err)}
		var typed *Failure
		if errors.As(err, &typed) && typed != nil {
			failure = typed
		}
		frame = map[string]any{"id": id, "error": failure}
	}
	if sendErr := p.send(frame); sendErr != nil {
		if p.ctx.Err() != nil {
			return
		}
		p.notify(p.OnCallFail.Emit(ctx, CallFailure{Invocation: call, Error: sendErr}))
		// A JSON-incompatible result must become a remote error, not a hung call.
		if fallbackErr := p.send(map[string]any{"id": id, "error": &Failure{Type: "SerializationError", Message: fmt.Sprint(sendErr)}}); fallbackErr != nil {
			_ = p.Close()
		}
		return
	}
	if err == nil {
		p.notify(p.OnCallSuccess.Emit(ctx, CallSuccess{Invocation: call, Result: result}))
	}
}

func (p *Peer) acquireJob() bool {
	select {
	case p.jobs <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *Peer) event(fn func()) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	if !p.acquireJob() {
		p.mu.Unlock()
		return ErrCapacity
	}
	p.workers.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.workers.Done()
		defer func() { <-p.jobs }()
		defer func() {
			if v := recover(); v != nil {
				p.notify(fmt.Errorf("event callback panic: %v", v))
			}
		}()
		fn()
	}()
	return nil
}

func (p *Peer) notify(err error) {
	if err != nil {
		_ = p.OnError.Emit(p.ctx, err)
	}
}

// Done closes when the connection ends; it is independent of a single call timeout.
func (p *Peer) Done() <-chan struct{} { return p.ctx.Done() }

// Context is the connection lifetime, not an individual invocation. Inside a
// handler use its ctx argument to preserve the current call metadata/deadline.
func (p *Peer) Context() context.Context { return p.ctx }

// Handle adds or replaces a route on one live connection.
func (p *Peer) Handle[P, R any](name string, fn func(context.Context, P) (R, error)) error {
	if !p.ready.Load() {
		return ErrNotReady
	}
	r := new(Router)
	if err := r.Handle(name, fn); err != nil {
		return err
	}
	p.routesMu.Lock()
	p.methods[name] = r.methods[name]
	p.routesMu.Unlock()
	return nil
}
func (p *Peer) DeleteRoute(name string) bool {
	p.routesMu.Lock()
	defer p.routesMu.Unlock()
	_, exists := p.methods[name]
	delete(p.methods, name)
	return exists
}

func (p *Peer) PendingCalls() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.pending) }
func (p *Peer) ActiveCalls() int  { p.mu.Lock(); defer p.mu.Unlock(); return len(p.active) }

func invoke(ctx context.Context, fn handler, raw json.RawMessage) (result any, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &Failure{Type: "PanicError", Message: fmt.Sprint(value)}
		}
	}()
	if fn == nil {
		return nil, &Failure{Type: "MethodNotFound", Message: "method not exposed"}
	}
	return fn(ctx, raw)
}

// Close cancels connection-scoped work and releases pending callers immediately.
// Handler cancellation is cooperative; Wait waits for handlers that honor ctx.
func (p *Peer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.cancel()
	p.pending = make(map[uint64]chan response)
	p.mu.Unlock()
	return p.transport.Close()
}

// Wait must follow Close. Cleanup runs after all active calls, once per peer.
func (p *Peer) Wait() {
	<-p.writerDone
	p.workers.Wait()
	p.finishOnce.Do(func() {
		if p.cleanup != nil {
			func() {
				defer func() {
					if v := recover(); v != nil {
						p.notify(fmt.Errorf("cleanup panic: %v", v))
					}
				}()
				p.cleanup()
			}()
		}
		p.notify(p.OnClose.Emit(p.ctx, p))
	})
}

// Remote represents a dotted namespace; Go does not have dynamic property proxies.
type Remote struct {
	peer      *Peer
	namespace string
}

// Method binds both argument and result types for a reusable client contract.
type Method[P, R any] struct {
	remote Remote
	name   string
}

func (p *Peer) Method[P, R any](name string) Method[P, R] {
	return Method[P, R]{remote: p.Remote(""), name: name}
}
func (m Method[P, R]) Call(ctx context.Context, params P) (R, error) {
	return m.remote.Call[R](ctx, m.name, params)
}

func (p *Peer) Remote(namespace string) Remote { return Remote{peer: p, namespace: namespace} }
func (r Remote) Call[R any](ctx context.Context, method string, params any) (R, error) {
	if r.namespace != "" {
		method = r.namespace + "." + method
	}
	return r.peer.Call[R](ctx, method, params)
}
