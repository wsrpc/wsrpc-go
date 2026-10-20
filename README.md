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

## Server

Register bound methods on already initialized objects. Only explicitly
registered methods are exposed; other exported methods stay private to your app.

```go
package main

import (
    "context"
    "log"
    "net/http"

    wsrpc "github.com/wsrpc/wsrpc-go"
    "github.com/wsrpc/wsrpc-go/ws"
)

type Echo struct{ Prefix string }
type Params struct{ Text string `json:"text"` }
type Result struct{ Text string `json:"text"` }

func (s *Echo) Call(_ context.Context, p Params) (Result, error) {
    return Result{Text: s.Prefix + p.Text}, nil
}

func main() {
    routes := new(wsrpc.Router)
    service := &Echo{Prefix: "Hello, "}
    if err := routes.Handle("echo", service.Call); err != nil { log.Fatal(err) }
    rpc, err := ws.NewServer(ws.ServerOptions{Peer: wsrpc.Options{Routes: routes}})
    if err != nil { log.Fatal(err) }
    mux := http.NewServeMux()
    mux.Handle("/ws/", rpc)
    log.Fatal(http.ListenAndServe("127.0.0.1:8000", mux))
}
```

The server implements `http.Handler`, so it also mounts on net/http-compatible
routers. Authentication runs in HTTP middleware or `ServerOptions.Authorize`
before the upgrade. Request context values remain available to route handlers.
Origin checks are enabled; use `OriginPatterns` for permitted cross-origin hosts.

Run complete examples:

```sh
go run ./examples/quickstart
# Or the richer echo/callback/counter demo:
go run ./examples/server
```

## Client and reverse calls

```go
routes := new(wsrpc.Router)
type Value struct { Value int `json:"value"` }
err := routes.Handle("double", func(_ context.Context, p Value) (int, error) {
    return p.Value * 2, nil
})
if err != nil { return err }

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
peer, err := ws.Dial(ctx, "ws://127.0.0.1:8000/ws/", wsrpc.Options{Routes: routes})
if err != nil { return err }
defer func() { _ = peer.Close(); peer.Wait() }()

result, err := peer.Call[int](ctx, "callback", Value{Value: 21}) // 42
```

Inside a handler, `wsrpc.FromContext(ctx)` returns the calling peer. That peer
can call the remote client while the original request is still running. The
reader stays live during handlers and events, including nested callbacks.
The example [client](examples/client/main.go) runs against Python, TS, or Go.

`Dial` uses its context for the handshake. Each call receives its own context;
cancelling a call's local wait keeps the connection open. Use `Close` to end the
connection. The shared wire protocol has no remote cancellation frame, so a
local timeout does not forcibly cancel remote application work.

## Typed contracts and namespaces

Declare a route's name and parameter/result types once and share that contract
between server and client. No schema language, generated code, or reflection is
needed:

```go
var Add = wsrpc.Define[Value, int]("add").In("counter")

// Server: register a method on the existing stateful object.
err := Add.Handle(routes, counter.Add)

// Client: both arguments and results follow the same contract.
value, err := Add.Call(ctx, peer, Value{Value: 2})
// Add.Call(ctx, peer, "wrong") does not compile.

// Or bind once for repeated calls on a connection:
add := Add.Bind(peer)
value, err = add.Call(ctx, Value{Value: 3})
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

For local context values, use one shared generic key instead of type assertions:

```go
var UserKey = wsrpc.NewContextKey[User]("user")

// In ServerOptions.Authorize, after application authentication:
ctx := UserKey.With(request.Context(), user)
// Return ctx from Authorize. In Factory: UserKey.Get(peer.Context()).
// In a handler or helper:
user, ok := UserKey.Get(ctx)
```

Keys have unique identities even when their diagnostic names match. `With`
preserves parent cancellation and supports nil/zero values; `Get` distinguishes
an absent value from a stored nil/zero. Mutable values still need synchronization.
Context values are local to each endpoint and are never serialized into calls.

Run the [objects/context demo](examples/objects/README.md) to follow HTTP context
through the connection factory, stateful methods and a reverse call:

```sh
go run ./examples/objects
# Another terminal:
go run ./examples/objects -url 'ws://127.0.0.1:8000/ws/?name=alice'
```

## Events, errors and shutdown

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
