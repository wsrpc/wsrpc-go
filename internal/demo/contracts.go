package demo

import wsrpc "github.com/wsrpc/wsrpc-go"

// These names and types are shared by the Go demo server and client. Echo stays
// dynamic to preserve the arbitrary JSON payloads of the Python/JS demos.
var (
	Echo     = wsrpc.Define[map[string]any, map[string]any]("echo")
	Double   = wsrpc.Define[Value, int]("double")
	Reverse  = wsrpc.Define[Value, int]("reverse")
	Callback = wsrpc.Define[Value, int]("callback")
	Deferred = wsrpc.Define[Value, int]("deferred")
	Init     = wsrpc.Define[Value, int]("init")
	Add      = wsrpc.Define[Value, int]("add")
)
