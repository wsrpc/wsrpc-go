package wsrpc

import "context"

// ContextKey stores a typed local context value. Use a shared key instance to
// pass connection data from HTTP authorization through factories and handlers.
// Values are not sent to the remote endpoint. Copies of pointer values still
// reference the same object; synchronize any mutable data accessed concurrently.
type ContextKey[T any] struct{ name string }

// NewContextKey creates an identity distinct from every other key, even keys
// with the same name and type. The name is for diagnostics only.
func NewContextKey[T any](name string) *ContextKey[T] {
	return &ContextKey[T]{name: name}
}

func (k *ContextKey[T]) String() string { return k.name }

type contextValue[T any] struct{ value T }

// With derives a context carrying value. It preserves the parent's cancellation
// and other values and supports nil values, including when T is an interface.
func (k *ContextKey[T]) With(parent context.Context, value T) context.Context {
	if k == nil {
		panic("wsrpc: nil context key")
	}
	return context.WithValue(parent, k, contextValue[T]{value: value})
}

// Get returns the value and whether this key is present. An absent key returns
// T's zero value and false; a stored zero or nil value returns true.
func (k *ContextKey[T]) Get(ctx context.Context) (T, bool) {
	value, ok := ctx.Value(k).(contextValue[T])
	return value.value, ok
}
