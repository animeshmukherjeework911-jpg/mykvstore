package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	store := NewStore()
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("failed to bind: %v", err)
	}
	defer ln.Close()
	fmt.Println("mykvstore listening on :6379")

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
