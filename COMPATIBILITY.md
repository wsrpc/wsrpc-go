# Python / TypeScript / Go compatibility

The Go implementation preserves WSRPC messages and behaviors across languages.
Public source APIs use the host language's conventions.

| Capability                                 | Go mapping                                                        | Evidence                                                            |
| ------------------------------------------ | ----------------------------------------------------------------- | ------------------------------------------------------------------- |
| Bidirectional calls and nested callbacks   | `Peer.Call[R]`, reader independent of handlers                    | Race tests; real aiohttp/ASGI/TS interop                            |
| Existing browser applications              | Same request/result/error/event frames                            | Unmodified npm 4.4.1 and HTML application in Chromium               |
| Routes, dotted names and bare `init` calls | Explicit `Handle` / `Mount`; `init` alias                         | Demo counter tested by all existing clients                         |
| Already initialized objects and DI         | Register bound Go methods on the same pointer                     | 40 concurrent reverse calls through one shared object               |
| Per-connection route objects               | `Options.Factory`; synchronized receiver state                    | Two live connections have separate counters                         |
| Call context                               | Explicit `context.Context`, `Current` and `FromContext`           | Isolation across calls, peers and helper goroutines                 |
| Typed handlers and results                 | Go 1.27 generic methods, `Method[P,R]`                            | Positive builds and three negative consumer compilations            |
| Generated protobuf/gRPC Go contracts       | Optional `wsrpcgrpc`, standard generated unary clients/services   | Generated code over real WebSockets; reverse calls, protojson/status tests |
| Dynamic JS/Python proxy syntax             | `Remote(namespace).Call[R]` or typed method handles               | Namespaced call in race suite                                       |
| Exposure / noproxy / prefix routes         | Explicit allowlist; application chooses names                     | Unregistered methods rejected, no reflection exposure               |
| Runtime route replacement/removal          | `Peer.Handle`, `DeleteRoute`                                      | Replacement/deletion test; in-flight handlers retain their binding  |
| Lifecycle and shared application hooks     | Typed signals and connection cleanup                              | Listener panic isolation and cleanup exactly once                   |
| Broadcast                                  | Per-peer typed outcomes                                           | Two distinct reverse-call results                                   |
| Local cancellation and timeout             | Caller context; remote operation not forcibly cancelled           | Blocked-write deadline and harmless late-response tests             |
| Server upgrade integration                 | `http.Handler`, authorize-before-upgrade, accepted socket adapter | Origin, authorization metadata, payload and shutdown tests          |
| Binary messages and events                 | Separate binary channel; events have no `id`                      | Live round-trip, event-triggered reverse call, browser binary event |
| JSON values                                | Raw presence detection, typed decode, `json.Number`               | Zero/false/empty/large number tests and Unicode interop             |
| Serialization extension                    | Standard `json.Marshaler` / `Unmarshaler`                         | Standard Go encoding; arbitrary alternate wire codecs not exposed   |

Important semantic differences:

- Go has no supported ambient goroutine-local equivalent of contextvars or
  AsyncLocalStorage. The approved design passes context explicitly. Do not use
  goroutine-ID hacks or mutate a shared service's socket pointer.
- Factory setup runs once at connection establishment. Bare route calls invoke
  `init` only when explicitly requested. Applications needing lazy construction
  can put `sync.Once` in a connection-scoped route object.
- Handlers, events and connections execute in goroutines. Shared state needs
  ordinary Go synchronization. Cancellation is cooperative, not forced termination.
- Go cannot synthesize typed property access such as `peer.proxy.counter.add`.
  Typed Go wrappers may use `Method[P,R]`; a code generator is not required by the
  runtime. The optional `wsrpcgrpc` adapter accepts standard `protoc-gen-go` and
  `protoc-gen-go-grpc` output for named unary calls and server interfaces.
- JSON structs use zero values for missing fields and ignore unknown fields.
  Required-field and business validation belongs to handlers. JSON object
  parameter names come from tags rather than Go function argument names.
- `[]byte` uses base64 in JSON; raw WebSocket binary is separate. Large integers
  that must survive a JavaScript receiver should be encoded as strings.
- Reconnection belongs to the host application: `Dial` creates one connection.
  The existing browser client's reconnect behavior remains untouched and is
  exercised against the Go server.

The transport-independent core can be adapted to another Go WebSocket library
without changing routes or messages. HTTP adapters for non-net/http frameworks
and alternate wire codecs remain extension points. The optional
[`wsrpcgrpc` adapter](wsrpcgrpc/README.md) provides generated Go unary stubs over
WSRPC, with explicit limits on streaming, metadata and remote cancellation;
it does not implement the gRPC HTTP/2 wire protocol.

Language references: [Go contexts](https://pkg.go.dev/context),
[goroutine identity](https://go.dev/doc/faq#goroutines),
[generic methods](https://go.dev/doc/go1.27),
[encoding/json](https://pkg.go.dev/encoding/json).
