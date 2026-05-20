# Stage 4 — In-memory store: SET, GET, DEL

Now that the protocol layer is solid, you can add the actual key-value store. In this stage you create a `Store` struct backed by a `map[string]string` and protect it with a `sync.RWMutex` so multiple client goroutines can safely read and write concurrently. You implement `SET`, `GET`, and `DEL`.

---

## Sub-step A — Understand sync.RWMutex

Go maps are not safe for concurrent use. Two goroutines writing simultaneously, or one writing while another reads, causes a data race that can corrupt the map or crash the process. You need a mutex.

`sync.RWMutex` has two lock modes:
- `Lock()` / `Unlock()` — exclusive write lock. Only one goroutine at a time. Used for SET, DEL.
- `RLock()` / `RUnlock()` — shared read lock. Many goroutines can hold this simultaneously, but not while a write lock is held. Used for GET.

This is the classic readers-writer lock and it improves throughput for workloads that are read-heavy, which key-value stores typically are.

---

## Sub-step B — Create store.go

```go
package main

import (
	"fmt"
	"sync"
)

// Store is a thread-safe in-memory key-value store.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Set stores key → value. Overwrites any existing value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for key and whether it exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Del removes the key. Returns true if the key existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

// Len returns the number of keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// String is for debugging only.
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store(%d keys)", len(s.data))
}
```

Using `defer s.mu.Unlock()` immediately after `s.mu.Lock()` is idiomatic Go. It ensures the lock is released even if the function panics, and it makes the lock scope visually obvious.

---

## Sub-step C — Wire the store into main.go

```go
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func main() {
	store := NewStore()

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("failed to bind: %v", err)
	}
	defer ln.Close()
	fmt.Println("mykvstore listening on :6379")

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
```

The `*Store` pointer is created once in `main` and shared across all goroutines. The mutex inside `Store` serializes access.

---

## Sub-step D — Add SET, GET, DEL to handler.go

```go
package main

import (
	"fmt"
	"strings"
)

func dispatch(parts []string, store *Store) string {
	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	switch cmd {
	case "PING":
		if len(args) == 0 {
			return encodeSimpleString("PONG")
		}
		return encodeBulkString(args[0])

	case "ECHO":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'echo' command")
		}
		return encodeBulkString(args[0])

	case "SET":
		if len(args) < 2 {
			return encodeError("wrong number of arguments for 'set' command")
		}
		store.Set(args[0], args[1])
		return encodeSimpleString("OK")

	case "GET":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'get' command")
		}
		val, ok := store.Get(args[0])
		if !ok {
			return encodeNull()
		}
		return encodeBulkString(val)

	case "DEL":
		if len(args) == 0 {
			return encodeError("wrong number of arguments for 'del' command")
		}
		// DEL accepts multiple keys; count how many were deleted.
		var deleted int64
		for _, key := range args {
			if store.Del(key) {
				deleted++
			}
		}
		return encodeInteger(deleted)

	case "COMMAND":
		return "*0\r\n"

	default:
		return encodeError(fmt.Sprintf("unknown command '%s'", parts[0]))
	}
}
```

Note that `DEL` accepts multiple keys in real Redis and returns the count of keys actually deleted. Implementing this correctly from the start saves a refactor later.

---

## Sub-step E — Test with redis-cli

```bash
go run .
```

```bash
redis-cli -p 6379 SET foo bar
# OK

redis-cli -p 6379 GET foo
# "bar"

redis-cli -p 6379 GET nosuchkey
# (nil)

redis-cli -p 6379 DEL foo
# (integer) 1

redis-cli -p 6379 DEL foo
# (integer) 0  (already gone)

redis-cli -p 6379 GET foo
# (nil)

redis-cli -p 6379 SET k1 a
redis-cli -p 6379 SET k2 b
redis-cli -p 6379 DEL k1 k2 k3
# (integer) 2  (k3 did not exist)
```

---

## Sub-step F — Detect data races with the race detector

Run with the race detector enabled to confirm your mutex usage is correct:

```bash
go run -race .
```

Open multiple terminals and hammer the server:

```bash
for i in $(seq 1 100); do redis-cli -p 6379 SET key$i val$i; done &
for i in $(seq 1 100); do redis-cli -p 6379 GET key$i; done &
```

With correct mutex usage, the race detector reports nothing. If you accidentally remove a lock, it will print a data race report and exit.

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `sync.RWMutex` | Protecting `Store.data` from concurrent access |
| `RLock`/`RUnlock` for reads | `Get`, `Len` |
| `Lock`/`Unlock` for writes | `Set`, `Del` |
| `defer mu.Unlock()` idiom | All Store methods |
| Go maps and `delete` builtin | `Store.Del` |
| RESP null bulk string (`$-1\r\n`) | `GET` for missing keys |
| RESP integer (`:N\r\n`) | `DEL` return value |
| Sharing state via pointer | `*Store` passed to every goroutine |
| `go run -race` | Verifying absence of data races |

---

## Checklist before moving to Stage 5

- [ ] `redis-cli -p 6379 SET foo bar` returns `OK`
- [ ] `redis-cli -p 6379 GET foo` returns `bar`
- [ ] `redis-cli -p 6379 GET missing` returns `(nil)`
- [ ] `redis-cli -p 6379 DEL foo` returns `(integer) 1`
- [ ] `redis-cli -p 6379 DEL foo` (again) returns `(integer) 0`
- [ ] Multi-key DEL returns the correct deleted count
- [ ] `go run -race .` shows no data race warnings under concurrent load
---
