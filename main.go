package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"mykvstore/internal/aof"
	"mykvstore/internal/handler"
	"mykvstore/internal/pubsub"
	"mykvstore/internal/rdb"
	"mykvstore/internal/resp"
	"mykvstore/internal/store"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	rdbPath := flag.String("rdb", rdb.DefaultRDBPath, "Path to RDB snapshot file")
	aofPath := flag.String("aof", aof.DefaultAOFPath, "Path to AOF log file")
	flag.Parse()

	s := store.NewStore()
	ps := pubsub.NewPubSub()

	rdbSavedAt, err := rdb.LoadRDB(s, *rdbPath)
	if err != nil {
		log.Fatalf("RDB load failed: %v", err)
	}

	aof, err := aof.OpenAOF(*aofPath)
	if err != nil {
		log.Fatalf("failed to open AOF: %v", err)
	}
	defer aof.Close()

	if err := aof.ReplayAfter(func(parts []string) { handler.Dispatch(nil, nil, parts, s, nil, nil) }, rdbSavedAt); err != nil {
		log.Fatalf("AOF replay failed: %v", err)
	}

	saver := rdb.NewRDBSaver(s, *rdbPath)
	saver.Start(60 * time.Second)
	defer saver.Stop()

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
		go handleConn(conn, s, aof, saver, ps)
	}
}

func handleConn(conn net.Conn, s *store.Store, aof *aof.AOF, saver *rdb.RDBSaver, ps *pubsub.PubSub) {
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

		response := handler.Dispatch(conn, r, parts, s, saver, ps)
		conn.Write([]byte(response))

		if isWriteCommand(parts[0]) {
			aof.Write(parts)
		}
	}
}

// isWriteCommand returns true for commands that mutate state and must be written to the AOF
func isWriteCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SET", "DEL", "EXPIRE", "PERSIST",
		"INCR", "INCRBY", "DECR", "DECRBY",
		"APPEND",
		"LPUSH", "RPUSH", "LPOP", "RPOP":
		return true
	}
	return false
}
