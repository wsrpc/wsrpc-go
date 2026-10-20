# Stateful objects, generic contracts and connection context

Run the server and a client in separate terminals:

```sh
go run ./examples/objects
go run ./examples/objects -url 'ws://127.0.0.1:8000/ws/?name=alice'
```

The client initializes its remote counter to 10, then adds 2 and 3. Results are
10, 12, 15, with the same server connection ID and user on every call. Running
another client creates a new object. The `name` query parameter is only a display
identity for the demo, not authentication.

The contract in [api.go](api.go) is shared by server and client:

```go
var Add = wsrpc.Define[Value, Snapshot]("add").In("counter")

// In the connection factory: keep the actual receiver, including dependencies.
err := Add.Handle(routes, counter.Add)

// On the client: the result is Snapshot and the argument must be Value.
add := Add.Bind(peer)
snapshot, err := add.Call(ctx, Value{Value: 2})
```

`Init` uses the bare `counter` route, matching WSRPC's object-initialization
convention. Binding a contract does not construct an object. The factory creates
one `Counter` per connection; `Init` sets its state. Every `Add` calls a method
on that same receiver, protected by a mutex. A ready `Audit` dependency is shared
across those objects. Factory cleanup logs the connection's release after its
handlers finish.

Follow the printed `factory`, `handler`, `callback`, and `cleanup` lines:

| Stage | Context and values |
| --- | --- |
| HTTP `Authorize` | `principalKey.With(r.Context(), Principal{Name: name})` adds a typed local identity. |
| Connection factory | `principalKey.Get(peer.Context())` reads that identity while constructing the receiver. HTTP request cancellation is detached at upgrade. |
| Incoming method | `principalKey.Get(ctx)` reads the identity; `wsrpc.Current(ctx)` adds the call ID, method and peer. |
| Server helper / reverse call | The handler passes its `ctx` explicitly to `Double.Call(ctx, peer, params)`. It controls the local wait; only the RPC method, ID and arguments go over the wire. |
| Client callback | Its handler receives the client's connection context and a new invocation. `clientKey.Get(ctx)` reads the value originally passed to `ws.Dial`; the server's principal is absent. |
| Cleanup | The factory closure retains this connection's identity and runs after its handlers finish. |

`peer.Context()` describes connection lifetime; handler `ctx` also contains
invocation metadata. Keep passing handler `ctx` to helpers rather than replacing
it with `context.Background()` or the bare connection context. Caller deadlines
and arbitrary context values are not transported to the remote endpoint.

No `.proto`, generated stubs, service reflection or gRPC dependency is involved.
The JSON WSRPC protocol and callback behavior stay the same.
