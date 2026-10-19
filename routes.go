package wsrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

type handler func(context.Context, json.RawMessage) (any, error)

// Router explicitly exposes selected methods, including bound methods of ready objects.
// Register routes before serving connections. A connection receives a snapshot.
type Router struct {
	mu      sync.RWMutex
	methods map[string]handler
}

// Handle uses Go 1.27 generic methods: parameters and results are inferred.
func (r *Router) Handle[P, R any](name string, fn func(context.Context, P) (R, error)) error {
	if !safeName(name) || fn == nil {
		return errors.New("invalid route registration")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.methods == nil {
		r.methods = make(map[string]handler)
	}
	if _, exists := r.methods[name]; exists {
		return fmt.Errorf("route already registered: %s", name)
	}
	r.methods[name] = func(ctx context.Context, raw json.RawMessage) (any, error) {
		var params P
		if err := decode(raw, &params); err != nil {
			return nil, &Failure{Type: "InvalidParams", Message: err.Error()}
		}
		return fn(ctx, params)
	}
	return nil
}

// Mount adds an explicit method set under a dotted namespace. It never uses reflection
// to expose all exported methods (which could include Close or administrative methods).
func (r *Router) Mount(prefix string, methods *Router) error {
	if !safeName(prefix) || methods == nil {
		return errors.New("invalid route namespace")
	}
	copy := methods.snapshot()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.methods == nil {
		r.methods = make(map[string]handler)
	}
	for name := range copy {
		if _, exists := r.methods[prefix+"."+name]; exists {
			return fmt.Errorf("route already registered: %s.%s", prefix, name)
		}
	}
	if _, init := copy["init"]; init {
		if _, exists := r.methods[prefix]; exists {
			return fmt.Errorf("route already registered: %s", prefix)
		}
	}
	for name, fn := range copy {
		r.methods[prefix+"."+name] = fn
	}
	if init, exists := copy["init"]; exists {
		r.methods[prefix] = init
	}
	return nil
}

func (r *Router) snapshot() map[string]handler {
	out := make(map[string]handler)
	if r == nil {
		return out
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for name, fn := range r.methods {
		out[name] = fn
	}
	return out
}

func safeName(name string) bool {
	for _, part := range strings.Split(name, ".") {
		if part == "" || strings.HasPrefix(part, "_") || part == "constructor" || part == "prototype" || part == "then" {
			return false
		}
	}
	return true
}

func decode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber() // Generic maps preserve decimal integers instead of rounding through float64.
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing JSON value")
	}
	return nil
}

func encode(value any) (data []byte, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("JSON encoding panic: %v", v)
		}
	}()
	return json.Marshal(value)
}

func makeRoutes(factory func(*Peer, *Router) (func(), error), p *Peer, r *Router) (cleanup func(), err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("route factory panic: %v", v)
		}
	}()
	return factory(p, r)
}

type callKey struct{}
type peerKey struct{}

// FromContext also works in connection event handlers, where no call ID exists.
func FromContext(ctx context.Context) (*Peer, bool) {
	p, ok := ctx.Value(peerKey{}).(*Peer)
	return p, ok
}

// Invocation is scoped to one incoming call, safe across concurrent calls and peers.
type Invocation struct {
	Peer   *Peer
	ID     uint64
	Method string
}

func Current(ctx context.Context) (Invocation, bool) {
	call, ok := ctx.Value(callKey{}).(Invocation)
	return call, ok
}
