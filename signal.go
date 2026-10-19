package wsrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Signal is a concurrency-safe set of lifecycle listeners. The returned function
// disconnects a listener. Every listener runs even if another fails or panics.
type Signal[T any] struct {
	mu        sync.RWMutex
	serial    uint64
	listeners map[uint64]func(context.Context, T) error
}

func (s *Signal[T]) Connect(fn func(context.Context, T) error) func() {
	if fn == nil {
		return func() {}
	}
	s.mu.Lock()
	if s.listeners == nil {
		s.listeners = make(map[uint64]func(context.Context, T) error)
	}
	s.serial++
	id := s.serial
	s.listeners[id] = fn
	s.mu.Unlock()
	return func() { s.mu.Lock(); delete(s.listeners, id); s.mu.Unlock() }
}

func (s *Signal[T]) Emit(ctx context.Context, value T) error {
	s.mu.RLock()
	copy := make([]func(context.Context, T) error, 0, len(s.listeners))
	for _, fn := range s.listeners {
		copy = append(copy, fn)
	}
	s.mu.RUnlock()
	var failures []error
	for _, fn := range copy {
		err := func() (err error) {
			defer func() {
				if v := recover(); v != nil {
					err = fmt.Errorf("signal listener panic: %v", v)
				}
			}()
			return fn(ctx, value)
		}()
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
