# Generated gRPC Go clients over WSRPC

`wsrpcgrpc` accepts ordinary output from `protoc-gen-go` and
`protoc-gen-go-grpc`. No custom generator or edits to generated code are needed.
Requires the same Go version as WSRPC. Importing this optional package brings in
gRPC and protobuf; the root WSRPC package still imports only the standard library.

Define the contract in a `.proto` file:

```proto
syntax = "proto3";
package helloworld;
option go_package = "example.org/app/pb";

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
}
message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Implement and register the generated server interface:

```go
type Greeter struct {
    pb.UnimplementedGreeterServer
}

func (*Greeter) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
    return &pb.HelloReply{Message: "Hello, " + req.Name}, nil
}

routes := new(wsrpc.Router)
pb.RegisterGreeterServer(wsrpcgrpc.NewRegistrar(routes), &Greeter{})
rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes}})
// Mount rpc as an http.Handler, after checking err.
```

Use the generated client with a connected WSRPC peer:

```go
client := pb.NewGreeterClient(wsrpcgrpc.NewClientConn(peer))
reply, err := client.SayHello(ctx, &pb.HelloRequest{Name: "World"})
```

Either endpoint can register services. In a handler, `wsrpc.FromContext(ctx)`
provides the peer for generated reverse calls. Registration happens before
connections take their router snapshot. For per-connection services, register
inside `wsrpc.Options.Factory`. Invalid or conflicting registrations panic;
the router stays unchanged when a registration fails.

## Runnable example and generation

The [example](../examples/grpc/main.go) contains both a server and a client
with a generated reverse call. Generated files are checked in, so running it
requires no `protoc` installation:

```sh
go run ./examples/grpc
# In another terminal:
go run ./examples/grpc -url ws://127.0.0.1:8000/ws/
# Hello from server, Go
# Hello from client, Go
```

To regenerate the example from the repository root, install `protoc` and the
standard Go plugins (the checked-in output uses protoc 35.1):

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
# Put the Go binary installation directory on PATH, then:
protoc -I . \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  examples/grpc/helloworld/hello.proto
```

Without a separate `protoc` installation, replace `protoc` with
`uv run --with grpcio-tools==1.84.0 python -m grpc_tools.protoc`.
CI uses that pinned compiler and checks that regeneration leaves no diff.

## Transport behavior and limits

- Unary calls only. Creating a stream returns `codes.Unimplemented`.
  Registering a service with any streaming methods panics before installing it.
- Standard full method `/helloworld.Greeter/SayHello` maps to WSRPC route
  `helloworld.Greeter.SayHello`. Plain WSRPC clients can call that route too.
  WSRPC's restrictions on reserved route components still apply.
- Requests and replies use the protobuf JSON mapping (`protojson`), including
  decimal strings for 64-bit integers, base64 bytes, enum names, and oneof presence. Unknown JSON
  fields are rejected. Protobuf binary unknown fields do not survive protojson.
- Status codes, messages, and opaque `Any` error details survive round trips.
  Non-status handler errors become `codes.Unknown`. The WSRPC error shape stays
  `{type, message}`: type is `GRPCStatus`, and message is a JSON-encoded
  `google.rpc.Status` Go struct (details use `type_url` and base64 `value`).
  This error encoding is specific to this adapter, not gRPC's HTTP/2 encoding.
- Caller cancellation/deadlines stop the local wait only. They are not sent to
  the remote handler, which receives the WSRPC invocation/connection context.
  Cancellation does not close the shared connection.
- Metadata, headers/trailers, call credentials, compression, retries, interceptors,
  and gRPC transport/context helpers are not implemented. Nonempty outgoing
  metadata and call options other than generated `grpc.StaticMethod()` are
  explicitly rejected with `codes.Unimplemented`.
- Connection lifecycle, authentication, origin checks, queue limits, and message
  limits come from WSRPC and its WebSocket adapter. A `ClientConn` borrows its peer;
  the caller still owns closing it.

This is generated-code compatibility for Go over the WSRPC JSON protocol. An
ordinary gRPC HTTP/2 client cannot connect to this WebSocket endpoint directly.
Generated gRPC clients for other languages need their own transport adapters.
