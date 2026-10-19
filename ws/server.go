package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	wsrpc "github.com/wsrpc/wsrpc-go"
)

type ServerOptions struct {
	Peer wsrpc.Options
	// Authorize executes before upgrading; returned request values survive the
	// upgrade, but its HTTP cancellation is replaced by connection lifetime.
	Authorize       func(*http.Request) (context.Context, error)
	OriginPatterns  []string
	MaxMessageBytes int64
	WriteTimeout    time.Duration
}

type Server struct {
	OnConnect    wsrpc.Signal[*wsrpc.Peer]
	OnDisconnect wsrpc.Signal[*wsrpc.Peer]
	OnError      wsrpc.Signal[error]
	options      ServerOptions
	mu           sync.Mutex
	closed       bool
	clients      map[*wsrpc.Peer]struct{}
	connecting   sync.WaitGroup
}

func NewServer(options ServerOptions) (*Server, error) {
	if options.MaxMessageBytes < 0 || options.WriteTimeout < 0 || options.Peer.MaxPending < 0 || options.Peer.MaxConcurrent < 0 || options.Peer.MaxQueuedBytes < 0 {
		return nil, errors.New("negative server limit")
	}
	if options.MaxMessageBytes == 0 {
		options.MaxMessageBytes = 1 << 20
	}
	return &Server{options: options, clients: make(map[*wsrpc.Peer]struct{})}, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		http.Error(w, "RPC server closed", http.StatusServiceUnavailable)
		return
	}
	ctx := r.Context()
	if s.options.Authorize != nil {
		var err error
		ctx, err = s.options.Authorize(r)
		if err != nil || ctx == nil {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.options.OriginPatterns})
	if err != nil {
		return
	}
	conn.SetReadLimit(s.options.MaxMessageBytes)
	ctx = context.WithoutCancel(ctx)
	options := s.options.Peer
	options.Client = false
	p, err := wsrpc.NewPeer(ctx, transport{conn: conn, writeTimeout: s.options.WriteTimeout}, options)
	if err != nil {
		_ = conn.CloseNow()
		_ = s.OnError.Emit(ctx, err)
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = p.Close()
		p.Wait()
		return
	}
	s.clients[p] = struct{}{}
	s.connecting.Add(1)
	s.mu.Unlock()
	p.OnError.Connect(func(ctx context.Context, err error) error { return s.OnError.Emit(ctx, err) })
	p.OnClose.Connect(func(ctx context.Context, peer *wsrpc.Peer) error {
		s.mu.Lock()
		delete(s.clients, peer)
		s.mu.Unlock()
		return s.OnDisconnect.Emit(ctx, peer)
	})
	go readLoop(p.Context(), conn, p)
	// The read loop must already be live if a connect listener calls the client.
	go func() {
		defer s.connecting.Done()
		if err := s.OnConnect.Emit(p.Context(), p); err != nil {
			_ = s.OnError.Emit(p.Context(), err)
		}
	}()
}

func (s *Server) Clients() []*wsrpc.Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*wsrpc.Peer, 0, len(s.clients))
	for p := range s.clients {
		result = append(result, p)
	}
	return result
}

type Outcome[T any] struct {
	Peer  *wsrpc.Peer
	Value T
	Error error
}

// Broadcast returns one outcome per connected peer, including individual errors.
func (s *Server) Broadcast[R any](ctx context.Context, method string, params any) []Outcome[R] {
	peers := s.Clients()
	results := make([]Outcome[R], len(peers))
	var wg sync.WaitGroup
	for i, p := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := p.Call[R](ctx, method, params)
			results[i] = Outcome[R]{Peer: p, Value: value, Error: err}
		}()
	}
	wg.Wait()
	return results
}

// Shutdown rejects new upgrades, closes connections, and waits for handler cleanup.
// Also call http.Server.Shutdown for ordinary HTTP connections and listeners.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	peers := s.Clients()
	for _, p := range peers {
		_ = p.Close()
	}
	done := make(chan struct{})
	go func() {
		for _, p := range peers {
			p.Wait()
		}
		s.connecting.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
