package wsrpc

import "context"

// Procedure is a shared RPC contract: a route name and its parameter/result types.
// It is independent of connections and safe to reuse across routers and peers.
// Like any Go type, it checks local code; it cannot verify a remote deployment's schema.
type Procedure[P, R any] struct{ name string }

// Define declares a contract. Invalid names panic because they are programming
// errors in a static contract. The zero Procedure is invalid for registration/calls.
func Define[P, R any](name string) Procedure[P, R] {
	if !safeName(name) {
		panic("wsrpc: invalid procedure name")
	}
	return Procedure[P, R]{name: name}
}

// Name returns the wire route name.
func (p Procedure[P, R]) Name() string { return p.name }

// In returns a contract in a dotted namespace without modifying the original.
// An empty namespace leaves the contract unchanged.
func (p Procedure[P, R]) In(namespace string) Procedure[P, R] {
	if namespace == "" {
		return p
	}
	return Define[P, R](namespace + "." + p.name)
}

// Handle registers a handler with exactly the contract's parameter/result types.
// Bound receiver methods retain their original object and injected dependencies.
func (p Procedure[P, R]) Handle(routes *Router, fn func(context.Context, P) (R, error)) error {
	return routes.Handle(p.name, fn)
}

// Bind returns a typed method bound to a peer. The caller owns the peer's lifetime.
func (p Procedure[P, R]) Bind(peer *Peer) Method[P, R] {
	return peer.Method[P, R](p.name)
}

// Call invokes this contract on a peer. ctx controls the local wait and is not
// serialized; the remote endpoint receives its own connection/invocation context.
func (p Procedure[P, R]) Call(ctx context.Context, peer *Peer, params P) (R, error) {
	return p.Bind(peer).Call(ctx, params)
}
