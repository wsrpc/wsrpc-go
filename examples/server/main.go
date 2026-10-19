package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wsrpc/wsrpc-go/internal/demo"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8000", "HTTP listen address")
	assets := flag.String("assets", "", "optional directory of demo static files")
	flag.Parse()
	rpc, err := demo.NewServer()
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/ws/", rpc)
	if *assets != "" {
		mux.Handle("/", http.FileServer(http.Dir(*assets)))
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ws://%s/ws/\n", listener.Addr())
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stopped
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rpc.Shutdown(ctx)
		_ = server.Shutdown(ctx)
	}()
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
