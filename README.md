# WSRPC Go

Bidirectional RPC over WebSockets, compatible with the WSRPC Python and
TypeScript implementations and existing JavaScript clients, including 4.4.1.
The server and client expose the same `Peer`: either endpoint can register
methods, make calls, return errors, and exchange events or binary messages.

Requires **Go 1.27+** for generic methods. The root `wsrpc` package uses the
standard library; the `ws` adapter uses
[`github.com/coder/websocket`](https://pkg.go.dev/github.com/coder/websocket).

```sh
go get github.com/wsrpc/wsrpc-go@main
```

## Complete server and client

This example has three files: a contract shared by both programs, a server, and
a client. Each connection owns a counter and a session. The session is available
through the same typed context key in the factory, handlers, and their helpers.
The server also calls a method registered by the client.

Create a module and the three directories:

```sh
mkdir rpcdemo
cd rpcdemo
go mod init example.com/rpcdemo
go get github.com/wsrpc/wsrpc-go@main
mkdir api server client
```

### Shared contract: `api/api.go`

Only argument/result types and route definitions are shared between programs.
The context keys and session objects below belong to each endpoint locally.

```go
package api

import wsrpc "github.com/wsrpc/wsrpc-go"

type Value struct {
    Value int `json:"value"`
}

type Snapshot struct {
    Value      int    `json:"value"`
    Doubled    int    `json:"doubled"`
    User       string `json:"user"`
    Calls      int    `json:"calls"`
    Connection string `json:"connection"`
}

var (
    Init   = wsrpc.Define[Value, Snapshot]("counter")
    Add    = wsrpc.Define[Value, Snapshot]("add").In("counter")
    Double = wsrpc.Define[Value, int]("double")
)
```

`Init` names the bare `counter` route, and `Add` names `counter.add`. Declaring a
contract does not construct an object: the server's factory controls that.
The compiler checks the server handler, client arguments, and client result
against the same `Procedure[P,R]` definition.

### Server: `server/main.go`

`Authorize` creates a session before the WebSocket upgrade. `Factory` constructs
one counter for that connection and injects a ready logger. Later handlers see
the same session pointer through their own invocation contexts. Both the counter
and the shared session protect mutable state with mutexes.

```go
package main

import (
    "context"
    "errors"
    "log"
    "net/http"
    "os"
    "sync"
    "time"

    "example.com/rpcdemo/api"
    wsrpc "github.com/wsrpc/wsrpc-go"
    "github.com/wsrpc/wsrpc-go/ws"
)

type Session struct {
    User  string // Immutable after authorization.
    mu    sync.Mutex
    Calls int
}

// Keep this one key instance: its name is a label, not a global lookup name.
var SessionKey = wsrpc.NewContextKey[*Session]("session")

type Counter struct {
    mu     sync.Mutex
    value  int
    logger *log.Logger // Initialized once, shared by all counter objects.
}

func (c *Counter) Init(ctx context.Context, p api.Value) (api.Snapshot, error) {
    if p.Value < 0 {
        return api.Snapshot{}, &wsrpc.Failure{
            Type: "InvalidValue", Message: "initial value must be nonnegative",
        }
    }
    c.mu.Lock()
    c.value = p.Value
    value := c.value
    c.mu.Unlock()
    return c.snapshot(ctx, value, 0)
}

func (c *Counter) Add(ctx context.Context, p api.Value) (api.Snapshot, error) {
    // Pass the incoming context to helpers. Do not hold the state mutex while
    // waiting for a reverse call: callbacks can make nested calls to this peer.
    doubled, err := c.doubleOnClient(ctx, p)
    if err != nil {
        return api.Snapshot{}, err
    }
    c.mu.Lock()
    c.value += p.Value
    value := c.value
    c.mu.Unlock()
    return c.snapshot(ctx, value, doubled)
}

func (c *Counter) doubleOnClient(ctx context.Context, p api.Value) (int, error) {
    session, ok := SessionKey.Get(ctx)
    if !ok {
        return 0, errors.New("missing session")
    }
    peer, ok := wsrpc.FromContext(ctx)
    if !ok {
        return 0, errors.New("missing calling peer")
    }
    c.logger.Printf("callback for user=%q peer=%s", session.User, peer.ID)
    callbackCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    return api.Double.Call(callbackCtx, peer, p)
}

func (c *Counter) snapshot(ctx context.Context, value, doubled int) (api.Snapshot, error) {
    session, ok := SessionKey.Get(ctx)
    if !ok {
        return api.Snapshot{}, errors.New("missing session")
    }
    call, ok := wsrpc.Current(ctx)
    if !ok {
        return api.Snapshot{}, errors.New("missing invocation")
    }
    session.mu.Lock()
    session.Calls++
    calls := session.Calls
    session.mu.Unlock()
    c.logger.Printf("user=%q peer=%s call=%d method=%s value=%d",
        session.User, call.Peer.ID, call.ID, call.Method, value)
    return api.Snapshot{
        Value: value, Doubled: doubled, User: session.User,
        Calls: calls, Connection: call.Peer.ID,
    }, nil
}

func main() {
    logger := log.New(os.Stdout, "rpc: ", log.LstdFlags)
    rpc, err := ws.NewServer(ws.ServerOptions{
        Authorize: func(r *http.Request) (context.Context, error) {
            // A display identity for the example; authenticate in real apps.
            name := r.URL.Query().Get("name")
            if name == "" {
                name = "guest"
            }
            return SessionKey.With(r.Context(), &Session{User: name}), nil
        },
        Peer: wsrpc.Options{
            Factory: func(peer *wsrpc.Peer, routes *wsrpc.Router) (func(), error) {
                session, ok := SessionKey.Get(peer.Context())
                if !ok {
                    return nil, errors.New("missing session")
                }
                logger.Printf("create counter for user=%q peer=%s", session.User, peer.ID)
                counter := &Counter{logger: logger}
                if err := api.Init.Handle(routes, counter.Init); err != nil {
                    return nil, err
                }
                if err := api.Add.Handle(routes, counter.Add); err != nil {
                    return nil, err
                }
                return func() {
                    logger.Printf("cleanup for user=%q peer=%s", session.User, peer.ID)
                }, nil
            },
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    mux := http.NewServeMux()
    mux.Handle("/ws/", rpc)
    log.Fatal(http.ListenAndServe("127.0.0.1:8000", mux))
}
```

Factories run before the reader starts. Register methods and initialize objects
there; start network calls from handlers or `rpc.OnConnect`, after the peer is
ready. Returning an error or a nil context from `Authorize` rejects the HTTP
upgrade. Only explicitly registered methods are exposed.

### Client: `client/main.go`

Register the callback before dialing, so it is available as soon as frames arrive.
The client puts its own value in the connection context. A callback gets that
local context, even though the server initiated the call.

```go
package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "time"

    "example.com/rpcdemo/api"
    wsrpc "github.com/wsrpc/wsrpc-go"
    "github.com/wsrpc/wsrpc-go/ws"
)

var ClientLabel = wsrpc.NewContextKey[string]("client label")

func main() {
    url := flag.String("url", "ws://127.0.0.1:8000/ws/?name=alice", "RPC endpoint")
    flag.Parse()
    if err := run(*url); err != nil {
        log.Fatal(err)
    }
}

func run(url string) error {
    routes := new(wsrpc.Router)
    if err := api.Double.Handle(routes, func(ctx context.Context, p api.Value) (int, error) {
        label, _ := ClientLabel.Get(ctx)
        call, _ := wsrpc.Current(ctx)
        fmt.Printf("callback: label=%q method=%s\n", label, call.Method)
        return p.Value * 2, nil
    }); err != nil {
        return err
    }

    connectionCtx := ClientLabel.With(context.Background(), "CLI connection")
    handshakeCtx, stopHandshake := context.WithTimeout(connectionCtx, 5*time.Second)
    peer, err := ws.Dial(handshakeCtx, url, wsrpc.Options{Routes: routes})
    stopHandshake() // Dial's deadline controls only the handshake.
    if err != nil {
        return err
    }
    defer func() { _ = peer.Close(); peer.Wait() }()

    // Each operation has a local wait deadline, independent of connection lifetime.
    ctx, cancel := context.WithTimeout(connectionCtx, 5*time.Second)
    defer cancel()
    init, add := api.Init.Bind(peer), api.Add.Bind(peer)
    first, err := init.Call(ctx, api.Value{Value: 10})
    if err != nil {
        return err
    }
    printSnapshot(first)
    for _, delta := range []int{2, 3} {
        next, err := add.Call(ctx, api.Value{Value: delta})
        if err != nil {
            return err
        }
        printSnapshot(next)
    }
    return nil
}

func printSnapshot(s api.Snapshot) {
    fmt.Printf("value=%d doubled=%d user=%s calls=%d connection=%s\n",
        s.Value, s.Doubled, s.User, s.Calls, s.Connection)
}
```

Start the server, then run clients in another terminal:

```sh
go run ./server
go run ./client
go run ./client -url 'ws://127.0.0.1:8000/ws/?name=bob'
```

The server command keeps running; issue the two client commands in the second
terminal. Each client prints the following, with its own user and connection ID:

```text
value=10 doubled=0 user=alice calls=1 connection=<same ID for these three calls>
callback: label="CLI connection" method=double
value=12 doubled=4 user=alice calls=2 connection=<same ID for these three calls>
callback: label="CLI connection" method=double
value=15 doubled=6 user=alice calls=3 connection=<same ID for these three calls>
```

Bob starts at 10 with `calls=1`; he does not inherit Alice's counter or session.
The factory's cleanup runs once after that connection closes and its handlers
finish. Return the object's resource-release function there when it owns resources.

### What the shared context means

A connection has a common **local parent context**, and each incoming call adds
its own `{Peer, ID, Method}` invocation. The factory, callbacks on that endpoint,
handlers, and helpers can read values inherited from that parent. Storing a
pointer such as `*Session` shares the same object, not a copy of its fields.

| Where | Context to use | Values and lifetime |
| --- | --- | --- |
| Server HTTP authorization | `r.Context()` → `SessionKey.With(...)` | One new session per accepted request. |
| Connection factory | `peer.Context()` | Same session; HTTP cancellation is detached at upgrade. |
| Incoming method/helper | The handler's `ctx` | Same session plus this call's invocation metadata. Derived contexts preserve both. |
| Server reverse call | `api.Double.Call(ctx, peer, params)` | `ctx` bounds the local wait. The wire carries the method, ID and parameters. |
| Client callback | The callback's `ctx` | Client connection values plus a new incoming invocation, not the server's session. |
| Connection close | `peer.Close()` followed by `peer.Wait()` | Cancels connection work, joins handlers and runs factory cleanup. |

`Snapshot.User` and `Snapshot.Calls` reach the client because the handler
explicitly returns those fields as RPC data. The `Session` pointer, context keys,
and arbitrary context values are not transmitted. The server cannot read
`ClientLabel`; a client callback cannot read the server's session.

Use the same key instance throughout an endpoint. Calling
`NewContextKey[*Session]("session")` again makes a distinct key, even with the same
label. `With` derives a context; it does not modify existing contexts. To share
mutable connection state, put one pointer in the parent before creating the peer
and synchronize its fields, as above. Pass the handler's `ctx` to helpers instead
of replacing it with `context.Background()` or `peer.Context()`; the latter loses
the individual call metadata.

Caller deadlines and cancellation are local too: timing out a wait does not
forcibly stop remote work or close the shared connection. Derive a server-side
timeout where a handler needs one, as `doubleOnClient` does.

Ready-to-run repository examples: [objects/context](examples/objects/README.md),
[quickstart](examples/quickstart/main.go), and the interoperable
[server](examples/server/main.go) / [client](examples/client/main.go).

## Typed contracts and namespaces

Declare a route's name and parameter/result types once and share that contract
between server and client. No schema language, generated code, or reflection is
needed:

```go
// Use the contract from api/api.go on both endpoints.
// Server: register a method on the existing stateful object.
err := api.Add.Handle(routes, counter.Add)

// Client: the argument is api.Value and the result is api.Snapshot.
snapshot, err := api.Add.Call(ctx, peer, api.Value{Value: 2})
// api.Add.Call(ctx, peer, "wrong") does not compile.

// Or bind once for repeated calls on a connection:
add := api.Add.Bind(peer)
snapshot, err = add.Call(ctx, api.Value{Value: 3})
```

`Procedure[P,R]` is an immutable value independent of a connection. `In` returns
a new descriptor with a dotted prefix; `Name` exposes its wire name. `Define`
panics for an invalid static name. `Handle` checks the handler signature at compile
time and returns an error for invalid/duplicate registration. The zero descriptor
is invalid. Binding a contract borrows a peer and creates no remote object.

The lower-level `Router.Handle`, `Peer.Call[R]`, `Peer.Method[P,R]`, and
`Remote(namespace).Call[R]` remain available for runtime-selected routes.
Go has no JavaScript-style dynamic property proxy. Named Go methods can wrap
these typed handles when your application needs `counter.Add(ctx, args)`.
Type information does not validate an independently deployed server's schema;
JSON decoding errors still return normally at runtime.

## Route objects and context

Shared initialized objects keep their dependencies and application state. They
receive `context.Context` for every call; no socket is swapped on the receiver.
Use mutexes or atomics for shared mutable state: Go handlers run concurrently.

For state owned by a connection, use `Options.Factory`. It runs once per peer,
adds routes to that connection's router, and returns an optional cleanup function.
See [Counter](internal/demo/routes.go) for a complete, synchronized example.

To share the tutorial's counter across connections, construct it once before
`ws.NewServer` and register it in the base router instead of in `Factory`:

```go
shared := &Counter{logger: logger}
routes := new(wsrpc.Router)
if err := api.Init.Handle(routes, shared.Init); err != nil { log.Fatal(err) }
if err := api.Add.Handle(routes, shared.Add); err != nil { log.Fatal(err) }
peerOptions := wsrpc.Options{Routes: routes}
// Use Peer: peerOptions in the same ServerOptions; keep Authorize unchanged.
```

Now all clients modify one counter, and `Init` resets it for everyone. Their
`*Session` objects remain distinct because `Authorize` still creates one per
connection. Object scope and context-value scope are controlled separately by
where the application constructs them.

`Router.Mount("counter", methods)` exposes a method set as `counter.add`, etc.
An explicitly registered `init` also accepts the bare `counter` name; construction
does not call it automatically. No reflection or automatic exposure of exported
methods occurs. `Peer.Handle` replaces a method on a live peer; `DeleteRoute`
removes it. Existing calls finish with the handler they already resolved.

`wsrpc.Current(ctx)` returns `{Peer, ID, Method}` inside an RPC handler or call
signal. Pass that context to helpers and goroutines explicitly. There is no
goroutine-local global equivalent of Python contextvars or TS AsyncLocalStorage.
`peer.Context()` is the connection lifetime and has no individual call metadata.
Register remote-call work in `Server.OnConnect`, after the reader starts;
factories configure routes and dependencies before network calls are allowed.

Register connection hooks before starting the HTTP server. For example, inside
the tutorial's server `main`, after constructing `rpc`:

```go
rpc.OnConnect.Connect(func(ctx context.Context, peer *wsrpc.Peer) error {
    session, _ := SessionKey.Get(ctx)
    _, hasCall := wsrpc.Current(ctx)
    logger.Printf("connected user=%q peer=%s hasCall=%t", session.User, peer.ID, hasCall)
    return nil // hasCall is false: this is a connection event, not an RPC call.
})
```

`ContextKey[T].With` preserves parent cancellation and supports nil/zero values;
`Get` distinguishes an absent value from a stored nil/zero. It checks the value
against the key's type at compile time. The full session example above uses
`ContextKey[*Session]` so helpers share one mutable object, protected by its mutex.

From the WSRPC Go repository, run the [objects/context demo](examples/objects/README.md)
to follow HTTP context through the factory, stateful methods and a reverse call:

```sh
go run ./examples/objects
# Another terminal:
go run ./examples/objects -url 'ws://127.0.0.1:8000/ws/?name=alice'
```

## Events, errors and shutdown

The server example returns `*wsrpc.Failure` when `Init` receives a negative
value. At a client call site, inspect remote failures separately from a local
timeout (add the standard `errors` import):

```go
_, err = init.Call(ctx, api.Value{Value: -1})
var remote *wsrpc.RemoteError
switch {
case errors.As(err, &remote):
    fmt.Println(remote.Error()) // initial value must be nonnegative
    fmt.Println(string(remote.Payload)) // {"type":"InvalidValue","message":...}
case errors.Is(err, context.DeadlineExceeded):
    fmt.Println("local wait timed out; the connection remains open")
case err != nil:
    return err
}
```

- `peer.Emit(ctx, map[string]any{...})` sends an event without an `id`.
- `peer.SendRaw(ctx, bytes, true)` sends a binary message.
- `OnEvent`, `OnBinary`, `OnCallStart`, `OnCallSuccess`, `OnCallFail`, `OnError`,
  and `OnClose` are typed signals. `Connect` returns an unsubscribe function.
- Listener panics/errors are isolated; `OnError` is the terminal reporting sink.
  Listeners must return or honor context cancellation, just like handlers.
- Returned errors become `{type, message}`; use `*wsrpc.Failure` to set the type.
  Remote errors are `*wsrpc.RemoteError` and retain the original JSON payload,
  including legacy JavaScript string errors.
- `server.Broadcast[R](ctx, method, params)` returns each peer's value or error.
- `server.Shutdown(ctx)` rejects new upgrades, closes peers and waits for cleanup.
  Also call `http.Server.Shutdown(ctx)` for ordinary HTTP connections/listeners.
- `peer.Close()` wakes pending callers and cancels handler contexts; `peer.Wait()`
  joins handlers and runs cleanup once. Call `Wait` after `Close`, outside handlers
  and signals belonging to that peer.

Defaults: 1 MiB incoming messages, 10 s writes, 1024 pending calls, 128 concurrent
incoming handlers/events, and 8 MiB queued/in-flight writes. Limits are configurable
through `ServerOptions` and `wsrpc.Options`; the outbound queue also holds at most
128 messages. Calls use the caller's deadline, so supply a bounded context.
Local timeouts do not interrupt another call or a shared transport write.

Use structs with `json` tags for stable public arguments/results. Maps decode
numbers as `json.Number`; IDs stay within JavaScript's safe integer range. Falsy
results and JSON null are preserved. `encoding/json` encodes `[]byte` as base64
inside JSON; use `SendRaw` for wire binary or explicit integer arrays as needed.
Unknown struct fields are ignored and missing fields get zero values: validate
required fields in your handler. Custom JSON marshaler/unmarshaler types work
with the standard codec; arbitrary non-JSON wire codecs are not provided.

## Development and versioning

```sh
go mod download
go vet ./...
go test -race ./...
npm ci --prefix interop
(cd interop && npx playwright install chromium)
uv run --locked --project tools/interop python tools/interop.py
```

Tests cover concurrency, context isolation, ready objects, per-peer factories,
callbacks, cancellation, limits, lifecycle and negative type compilation. Real
interop checks both directions with Python aiohttp/ASGI and TS, plus npm 4.4.1
and an unchanged browser application running in Chromium against the Go server.

One module versions the client, core and server together. Go obtains module
versions from Git tags; no version constant or bump commit is needed. Major v0/v1
use `github.com/wsrpc/wsrpc-go`; v2+ require the `/v2` suffix and updated imports.
No `v1/` directory is needed. See the
[Go module rules](https://go.dev/ref/mod#module-path) and
[Go 1.27 generic methods](https://go.dev/doc/go1.27).

Documentation: [docs.wsrpc.org](https://docs.wsrpc.org/go/).
Apache-2.0. Dmitry Orlov — <orlovdl@gmail.com>.
