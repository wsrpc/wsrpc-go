package main

import wsrpc "github.com/wsrpc/wsrpc-go"

type Value struct {
	Value int `json:"value"`
}

type Snapshot struct {
	Value      int    `json:"value"`
	User       string `json:"user"`
	Connection string `json:"connection"`
}

// Both endpoints use these contracts for registration and calls.
var (
	Init   = wsrpc.Define[Value, Snapshot]("counter")
	Add    = wsrpc.Define[Value, Snapshot]("add").In("counter")
	Double = wsrpc.Define[Value, int]("double")
)

type Principal struct{ Name string }

// Keys carry local values through context.Context; values never cross the wire.
var (
	principalKey = wsrpc.NewContextKey[Principal]("demo.principal")
	clientKey    = wsrpc.NewContextKey[string]("demo.client")
)
