package wsrpcgrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	wsrpc "github.com/wsrpc/wsrpc-go"
	pb "github.com/wsrpc/wsrpc-go/examples/grpc/helloworld"
	"github.com/wsrpc/wsrpc-go/ws"
	"github.com/wsrpc/wsrpc-go/wsrpcgrpc"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type greeter struct {
	pb.UnimplementedGreeterServer
	hello func(context.Context, *pb.HelloRequest) (*pb.HelloReply, error)
}

func (g *greeter) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	if g.hello != nil {
		return g.hello(ctx, req)
	}
	return &pb.HelloReply{Message: "Hello, " + req.Name, Echo: req}, nil
}

func (*greeter) Reverse(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	peer, ok := wsrpc.FromContext(ctx)
	if !ok {
		return nil, errors.New("lost peer context")
	}
	return pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer)).SayHello(ctx, req)
}

func routes(service pb.GreeterServer) *wsrpc.Router {
	r := new(wsrpc.Router)
	pb.RegisterGreeterServer(wsrpcgrpc.NewRegistrar(r), service)
	return r
}

func connect(t *testing.T, serverRoutes, clientRoutes *wsrpc.Router) *wsrpc.Peer {
	t.Helper()
	server, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: serverRoutes}})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(server)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		httpServer.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peer, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http"), wsrpc.Options{Routes: clientRoutes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close(); peer.Wait() })
	return peer
}

func TestGeneratedClientsOverWebSocket(t *testing.T) {
	serverRoutes := routes(&greeter{hello: func(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
		invocation, ok := wsrpc.Current(ctx)
		if !ok || invocation.Method != "helloworld.Greeter.SayHello" {
			return nil, errors.New("lost invocation context")
		}
		return &pb.HelloReply{Message: req.Name, Echo: req}, nil
	}})
	peer := connect(t, serverRoutes, routes(&greeter{}))
	client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Go(func() {
			req := &pb.HelloRequest{
				Name: fmt.Sprintf("Привет %d", i), Sequence: 9223372036854775807,
				Payload: []byte{0, 255, 1}, Tone: pb.HelloRequest_LOUD,
				Selection: &pb.HelloRequest_Index{Index: 0}, Tags: []string{"a", "b"},
				Attributes: map[string]string{"key": "value"},
			}
			reply, err := client.SayHello(ctx, req)
			if err != nil {
				t.Error(err)
				return
			}
			if reply.Message != req.Name || !proto.Equal(reply.Echo, req) {
				t.Errorf("protobuf fields changed: %v", reply)
			}
			reply, err = client.Reverse(ctx, req)
			if err != nil || reply.GetMessage() != "Hello, "+req.Name || !proto.Equal(reply.GetEcho(), req) {
				t.Errorf("reverse generated call: %v, %v", reply, err)
			}
		})
	}
	wg.Wait()
	// A plain WSRPC client uses the same route and protobuf JSON mapping.
	raw, err := peer.Call[json.RawMessage](ctx, "helloworld.Greeter.SayHello", json.RawMessage(`{"name":"raw","sequence":"9223372036854775807","tone":"LOUD","payload":"AP8B","label":""}`))
	if err != nil || !strings.Contains(string(raw), `"sequence":"9223372036854775807"`) || !strings.Contains(string(raw), `"label":""`) {
		t.Fatalf("protojson wire representation: %s, %v", raw, err)
	}
}

func TestStatusesAndInvalidMessages(t *testing.T) {
	detailed, err := status.New(codes.PermissionDenied, "denied").WithDetails(&errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "name", Description: "not allowed"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Unknown Any types must survive without a client-side type resolver.
	opaque := status.FromProto(&statuspb.Status{Code: int32(codes.Aborted), Message: "opaque", Details: []*anypb.Any{{TypeUrl: "example.invalid/Unknown", Value: []byte{1, 2, 3}}}})
	service := &greeter{hello: func(_ context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
		switch req.Name {
		case "status":
			return nil, detailed.Err()
		case "opaque":
			return nil, opaque.Err()
		case "plain":
			return nil, errors.New("ordinary failure")
		case "cancel":
			return nil, context.Canceled
		case "deadline":
			return nil, context.DeadlineExceeded
		case "nil":
			return nil, nil
		case "utf8":
			return &pb.HelloReply{Message: string([]byte{255})}, nil
		}
		return &pb.HelloReply{}, nil
	}}
	peer := connect(t, routes(service), nil)
	client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, code := range map[string]codes.Code{"status": codes.PermissionDenied, "opaque": codes.Aborted, "plain": codes.Unknown, "cancel": codes.Canceled, "deadline": codes.DeadlineExceeded, "nil": codes.Internal, "utf8": codes.Internal} {
		t.Run(name, func(t *testing.T) {
			_, err := client.SayHello(ctx, &pb.HelloRequest{Name: name})
			if status.Code(err) != code {
				t.Fatalf("got %v, want %v", err, code)
			}
			if name == "status" && !proto.Equal(status.Convert(err).Proto(), detailed.Proto()) {
				t.Fatalf("status details lost: %v", err)
			}
			if name == "opaque" && !proto.Equal(status.Convert(err).Proto(), opaque.Proto()) {
				t.Fatalf("opaque details lost: %v", err)
			}
		})
	}
	for _, raw := range []string{`{"sequence":"bad"}`, `{"unknown":true}`, `{"label":"a","index":1}`} {
		_, err := peer.Call[json.RawMessage](ctx, "helloworld.Greeter.SayHello", json.RawMessage(raw))
		var remote *wsrpc.RemoteError
		if !errors.As(err, &remote) {
			t.Fatalf("malformed request succeeded: %s, %v", raw, err)
		}
		var failure wsrpc.Failure
		var st statuspb.Status
		if json.Unmarshal(remote.Payload, &failure) != nil || json.Unmarshal([]byte(failure.Message), &st) != nil || st.Code != int32(codes.InvalidArgument) {
			t.Fatalf("wrong invalid-request status: %v", err)
		}
	}
}

func TestUnsupportedFeaturesAndValidation(t *testing.T) {
	peer := connect(t, routes(&greeter{}), nil)
	conn := wsrpcgrpc.NewClientConn(peer)
	client := pb.NewGreeterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, option := range []grpc.CallOption{grpc.Header(new(metadata.MD)), grpc.WaitForReady(true), grpc.MaxCallRecvMsgSize(1024), nil} {
		_, err := client.SayHello(ctx, &pb.HelloRequest{}, option)
		if status.Code(err) != codes.Unimplemented {
			t.Fatalf("silently accepted option %T: %v", option, err)
		}
	}
	_, err := client.SayHello(metadata.AppendToOutgoingContext(ctx, "authorization", "token"), &pb.HelloRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("silently accepted metadata: %v", err)
	}
	_, err = pb.NewStreamingGreeterClient(conn).Chat(ctx)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("silently accepted stream: %v", err)
	}
	for _, method := range []string{"bad", "/Greeter/", "/Greeter/A.B", "/Greeter/Name/Extra"} {
		if err := conn.Invoke(ctx, method, &pb.HelloRequest{}, &pb.HelloReply{}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("invalid method %q: %v", method, err)
		}
	}
	for _, value := range []any{nil, (*pb.HelloRequest)(nil), "wrong"} {
		if err := conn.Invoke(ctx, pb.Greeter_SayHello_FullMethodName, value, &pb.HelloReply{}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("invalid request %T: %v", value, err)
		}
		if err := conn.Invoke(ctx, pb.Greeter_SayHello_FullMethodName, &pb.HelloRequest{}, value); status.Code(err) != codes.InvalidArgument {
			t.Errorf("invalid reply %T: %v", value, err)
		}
	}
	if _, err := client.SayHello(ctx, &pb.HelloRequest{Name: string([]byte{255})}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid protobuf request was sent: %v", err)
	}
	if err := conn.Invoke(ctx, "/missing.Service/Call", &pb.HelloRequest{}, &pb.HelloReply{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("missing method: %v", err)
	}
	_ = peer.Close()
	if _, err := client.SayHello(ctx, &pb.HelloRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("closed peer: %v", err)
	}
}

func TestInvalidRepliesAndLegacyFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply json.RawMessage
		err   error
		code  codes.Code
	}{
		{name: "wrong schema", reply: json.RawMessage(`{"message":42}`), code: codes.Internal},
		{name: "malformed status", err: &wsrpc.Failure{Type: "GRPCStatus", Message: "not JSON"}, code: codes.Internal},
		{name: "error marked OK", err: &wsrpc.Failure{Type: "GRPCStatus", Message: `{"code":0}`}, code: codes.Internal},
		{name: "invalid status code", err: &wsrpc.Failure{Type: "GRPCStatus", Message: `{"code":99}`}, code: codes.Internal},
		{name: "legacy error", err: errors.New("legacy error"), code: codes.Unknown},
		{name: "remote capacity", err: &wsrpc.Failure{Type: "CapacityError", Message: "busy"}, code: codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := new(wsrpc.Router)
			if err := r.Handle("helloworld.Greeter.SayHello", func(context.Context, json.RawMessage) (json.RawMessage, error) {
				return tc.reply, tc.err
			}); err != nil {
				t.Fatal(err)
			}
			peer := connect(t, r, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer)).SayHello(ctx, &pb.HelloRequest{})
			if status.Code(err) != tc.code {
				t.Fatalf("got %v, want %v", err, tc.code)
			}
		})
	}
}

func TestLocalDeadlineKeepsConnectionAndRemoteWorkAlive(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	peer := connect(t, routes(&greeter{hello: func(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
		if req.Name == "slow" {
			close(started)
			<-release
			finished <- ctx.Err()
		}
		return &pb.HelloReply{Message: req.Name}, nil
	}}), nil)
	// Release remote work before cleanup waits for handlers, even after a failure.
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	localCtx, localCancel := context.WithCancel(ctx)
	defer localCancel()
	result := make(chan error, 1)
	go func() { _, err := client.SayHello(localCtx, &pb.HelloRequest{Name: "slow"}); result <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	localCancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("local cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer deadlineCancel()
	if _, err := client.SayHello(deadlineCtx, &pb.HelloRequest{}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("local deadline: %v", err)
	}
	if reply, err := client.SayHello(ctx, &pb.HelloRequest{Name: "still open"}); err != nil || reply.GetMessage() != "still open" {
		t.Fatalf("connection after cancellation: %v, %v", reply, err)
	}
	// Handler is still blocked, proving cancellation did not reach the server.
	select {
	case <-finished:
		t.Fatal("remote handler ended before release")
	default:
	}
	unblock()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("local cancellation reached remote context: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func mustPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("registration unexpectedly succeeded")
		}
	}()
	fn()
}

func TestRegistrationIsAtomicAndRejectsStreaming(t *testing.T) {
	r := new(wsrpc.Router)
	if err := r.Handle("helloworld.Greeter.Reverse", func(context.Context, json.RawMessage) (string, error) { return "existing", nil }); err != nil {
		t.Fatal(err)
	}
	registrar := wsrpcgrpc.NewRegistrar(r)
	mustPanic(t, func() { pb.RegisterGreeterServer(registrar, &greeter{}) })
	mustPanic(t, func() { pb.RegisterStreamingGreeterServer(registrar, &pb.UnimplementedStreamingGreeterServer{}) })
	mustPanic(t, func() { registrar.RegisterService(&pb.Greeter_ServiceDesc, "wrong implementation") })
	mustPanic(t, func() { registrar.RegisterService(nil, &greeter{}) })
	mustPanic(t, func() { wsrpcgrpc.NewRegistrar(nil) })
	mustPanic(t, func() { wsrpcgrpc.NewClientConn(nil) })
	peer := connect(t, r, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer)).SayHello(ctx, &pb.HelloRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("failed registration installed a partial service: %v", err)
	}
	value, err := peer.Call[string](ctx, "helloworld.Greeter.Reverse", nil)
	if err != nil || value != "existing" {
		t.Fatalf("failed registration changed existing method: %v, %v", value, err)
	}
}
