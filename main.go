package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	store := NewStore()
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
			store.evictExpired()
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, store)

	}
}

func handleConn(conn net.Conn, store *Store) {
	defer conn.Close()

	r := bufio.NewReader(conn)

	for {
		parts, err := readCommand(r)

		if err != nil {
			return
		}

		if len(parts) == 0 {
			continue
		}

		response := dispatch(parts, store)
		conn.Write([]byte(response))
	}

}
