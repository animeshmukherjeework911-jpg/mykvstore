package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"mykvstore/internal/handler"
	"mykvstore/internal/resp"
	"mykvstore/internal/store"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	s := store.NewStore()
	addr := ":" + *port
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to bind: %v", err)
	}
	defer ln.Close()
	fmt.Printf("mykvstore listening on %s\n", addr)

	// 100ms is a balance between eviction latency and lock contention on the store.
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			s.EvictExpired()
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, s)
	}
}

func handleConn(conn net.Conn, s *store.Store) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		parts, err := resp.ReadCommand(r)
		if err != nil {
			return
		}
		if len(parts) == 0 {
			continue
		}
		conn.Write([]byte(handler.Dispatch(parts, s)))
	}
}
