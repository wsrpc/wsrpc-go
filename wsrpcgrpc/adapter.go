// Package wsrpcgrpc runs standard generated gRPC Go unary clients and services
// over WSRPC. It implements the generated-code interfaces, not the gRPC wire protocol.
package wsrpcgrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	wsrpc "github.com/wsrpc/wsrpc-go"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const statusType = "GRPCStatus"

var (
	_ grpc.ClientConnInterface = (*ClientConn)(nil)
	_ grpc.ServiceRegistrar    = (*Registrar)(nil)
)

// ClientConn borrows a peer. Closing the peer remains the caller's responsibility.
// It is safe for concurrent calls. Do not mutate requests or access replies
// while a call uses them.
type ClientConn struct{ peer *wsrpc.Peer }

// NewClientConn wraps a connected peer for use by generated gRPC clients.
func NewClientConn(peer *wsrpc.Peer) *ClientConn {
	if peer == nil {
		panic("wsrpcgrpc: nil peer")
	}
	return &ClientConn{peer: peer}
}

// Invoke implements unary calls. Only the generated StaticMethod call option
// is supported. Other options and nonempty outgoing metadata are rejected.
// ctx controls the local wait; it is not transmitted to the remote handler.
func (c *ClientConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	for _, opt := range opts {
		if _, ok := opt.(grpc.StaticMethodCallOption); !ok {
			return status.Errorf(codes.Unimplemented, "wsrpcgrpc: unsupported call option %T", opt)
		}
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok && len(md) != 0 {
		return status.Error(codes.Unimplemented, "wsrpcgrpc: metadata is not supported")
	}
	name, err := routeName(method)
	if err != nil {
		return err
	}
	request, ok := message(args)
	if !ok {
		return status.Error(codes.InvalidArgument, "wsrpcgrpc: request must be a non-nil protobuf message")
	}
	response, ok := message(reply)
	if !ok {
		return status.Error(codes.InvalidArgument, "wsrpcgrpc: reply must be a non-nil protobuf message")
	}
	raw, err := protojson.Marshal(request)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "wsrpcgrpc: encode request: %v", err)
	}
	result, err := c.peer.Call[json.RawMessage](ctx, name, json.RawMessage(raw))
	if err != nil {
		return callError(err)
	}
	if err := protojson.Unmarshal(result, response); err != nil {
		return status.Errorf(codes.Internal, "wsrpcgrpc: decode reply: %v", err)
	}
	return nil
}

func (*ClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "wsrpcgrpc: streaming is not supported")
}

// Registrar installs services before peers take their route snapshots. Construct
// it inside Options.Factory for services with per-connection dependencies.
// Invalid, duplicate, or streaming registrations panic, like invalid gRPC server
// registrations. A failed registration leaves the router unchanged.
type Registrar struct{ routes *wsrpc.Router }

// NewRegistrar wraps a router for generated Register<Service>Server functions.
func NewRegistrar(routes *wsrpc.Router) *Registrar {
	if routes == nil {
		panic("wsrpcgrpc: nil router")
	}
	return &Registrar{routes: routes}
}

func (r *Registrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if desc == nil || len(desc.Streams) != 0 {
		panic("wsrpcgrpc: nil descriptor or unsupported streaming service")
	}
	t := reflect.TypeOf(desc.HandlerType)
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Interface || impl == nil || !reflect.TypeOf(impl).Implements(t.Elem()) {
		panic("wsrpcgrpc: implementation does not implement the service interface")
	}
	v := reflect.ValueOf(impl)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		panic("wsrpcgrpc: nil service implementation")
	}
	methods := new(wsrpc.Router)
	for _, method := range desc.Methods {
		if _, err := routeName("/" + desc.ServiceName + "/" + method.MethodName); err != nil || method.Handler == nil {
			panic("wsrpcgrpc: invalid method descriptor")
		}
		err := methods.Handle(method.MethodName, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			result, err := method.Handler(impl, ctx, func(target any) error {
				m, ok := message(target)
				if !ok {
					return status.Error(codes.Internal, "wsrpcgrpc: handler decoder requires a protobuf message")
				}
				if err := protojson.Unmarshal(raw, m); err != nil {
					return status.Errorf(codes.InvalidArgument, "wsrpcgrpc: decode request: %v", err)
				}
				return nil
			}, nil)
			if err != nil {
				return nil, wireError(err)
			}
			m, ok := message(result)
			if !ok {
				return nil, wireError(status.Error(codes.Internal, "wsrpcgrpc: handler returned a non-protobuf or nil reply"))
			}
			encoded, err := protojson.Marshal(m)
			if err != nil {
				return nil, wireError(status.Errorf(codes.Internal, "wsrpcgrpc: encode reply: %v", err))
			}
			return json.RawMessage(encoded), nil
		})
		if err != nil {
			panic(fmt.Sprintf("wsrpcgrpc: %v", err))
		}
	}
	if err := r.routes.Mount(desc.ServiceName, methods); err != nil {
		panic(fmt.Sprintf("wsrpcgrpc: %v", err))
	}
}

func message(value any) (proto.Message, bool) {
	m, ok := value.(proto.Message)
	return m, ok && m != nil && m.ProtoReflect().IsValid()
}

// Standard full method paths map to dotted WSRPC routes, including the proto package.
func routeName(fullMethod string) (string, error) {
	parts := strings.Split(fullMethod, "/")
	if len(parts) != 3 || parts[0] != "" || !identifier(parts[1], true) || !identifier(parts[2], false) {
		return "", status.Error(codes.InvalidArgument, "wsrpcgrpc: expected /package.Service/Method")
	}
	return parts[1] + "." + parts[2], nil
}

func identifier(name string, dotted bool) bool {
	for _, part := range strings.Split(name, ".") {
		if part == "" || strings.HasPrefix(part, "_") || part == "constructor" || part == "prototype" || part == "then" || (!dotted && part != name) {
			return false
		}
		for i, c := range part {
			if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9') {
				return false
			}
		}
	}
	return true
}

// The existing WSRPC error has only type and message. Keep its shape and put
// google.rpc.Status in message as JSON; Any details retain opaque protobuf bytes.
func wireError(err error) error {
	st := status.Convert(err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		st = status.FromContextError(err)
	}
	raw, _ := json.Marshal(st.Proto())
	return &wsrpc.Failure{Type: statusType, Message: string(raw)}
}

func callError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	if errors.Is(err, wsrpc.ErrClosed) || errors.Is(err, wsrpc.ErrNotReady) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, wsrpc.ErrCapacity) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	var remote *wsrpc.RemoteError
	if errors.As(err, &remote) {
		var failure wsrpc.Failure
		if json.Unmarshal(remote.Payload, &failure) == nil {
			if failure.Type == statusType {
				var st statuspb.Status
				if json.Unmarshal([]byte(failure.Message), &st) != nil || st.Code <= 0 || st.Code > int32(codes.Unauthenticated) {
					return status.Error(codes.Internal, "wsrpcgrpc: malformed remote status")
				}
				return status.FromProto(&st).Err()
			}
			switch failure.Type {
			case "MethodNotFound":
				return status.Error(codes.Unimplemented, failure.Message)
			case "CapacityError":
				return status.Error(codes.ResourceExhausted, failure.Message)
			}
		}
	}
	return status.Error(codes.Unknown, err.Error())
}
