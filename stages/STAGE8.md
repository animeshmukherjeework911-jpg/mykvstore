# Stage 8 — Persistence: Append-Only File (AOF)

So far the server loses all data on restart. In this stage you add an Append-Only File (AOF): every write command is appended to `aof.log` as a RESP-encoded string, and on startup the file is replayed to restore state. This is the same persistence strategy Redis uses in its `appendonly yes` mode.

---

## Implementation Status — COMPLETE

The AOF lives in `internal/aof/aof.go` (package `aof`), not in `package main`.

Key differences from the stage description:

| Stage description | Actual implementation |
|---|---|
| `const aofPath = "aof.log"` | `const AOF_PATH = "aof/aof.log"` (in a subdirectory) |
| `OpenAOF` opens a flat file | `OpenAOF` calls `os.MkdirAll` to create the `aof/` directory first |
| `aof.Replay(store *Store)` | `aof.Replay(store *store.Store)` — takes the imported package type |
| `ReplayAfter(store, cutoff)` method shown in Stage 9 | Not implemented — only `Replay` exists; Stage 9 uses the same `Replay` |
| `isWriteCommand` in `handler.go` | `isWriteCommand` in `main.go` |
| All code in `package main` | AOF code in `internal/aof`, referenced via import in `main.go` |

The `AOF_PATH` constant is exported (uppercase) and the file path uses a subdirectory (`aof/aof.log`) so the AOF file is grouped separately from the binary.

---

## Sub-step A — How AOF works

Every time a mutating command succeeds (SET, DEL, LPUSH, etc.) you write the exact RESP encoding of that command to a file opened with `O_APPEND`. Because appends to a file are atomic at the OS level up to a certain size, you never get partial writes interleaved between goroutines.

On startup you open the file, parse it as a stream of RESP commands, and re-run each one through `dispatch`. Because the commands are idempotent (SET foo bar replayed sets foo to bar again), the result is identical to the original state.

The trade-off:
- **Durability**: every write is recorded, so at most you lose the last unflushed write.
- **Recovery time**: replaying a large AOF is slow because you re-execute every command.
- **File size**: AOF grows without bound until you rewrite it (not covered here; Stage 9 addresses this with RDB snapshots).

---

## Sub-step B — Create aof.go

```go
package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
)

const aofPath = "aof.log"

// AOF handles append-only file persistence.
type AOF struct {
	mu   sync.Mutex
	file *os.File
	bw   *bufio.Writer
}

// OpenAOF opens (or creates) the AOF file for appending.
func OpenAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open aof: %w", err)
	}
	return &AOF{
		file: f,
		bw:   bufio.NewWriterSize(f, 4096),
	}, nil
}

// Write appends a RESP-encoded command to the AOF.
// parts is the slice of strings exactly as parsed from the client.
func (a *AOF) Write(parts []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Encode as RESP array.
	fmt.Fprintf(a.bw, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(a.bw, "$%d\r\n%s\r\n", len(p), p)
	}
	// Flush to the OS buffer. For stronger durability you would also call
	// a.file.Sync() here, but that has a significant performance cost.
	a.bw.Flush()
}

// Sync flushes and fsyncs the file to disk.
// Call this if you need a durability guarantee stronger than OS buffering.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.bw.Flush(); err != nil {
		return err
	}
	return a.file.Sync()
}

// Close flushes and closes the AOF file.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bw.Flush()
	return a.file.Close()
}

// Replay reads the AOF from the beginning and re-runs every command
// through the provided dispatch function to rebuild store state.
func (a *AOF) Replay(store *Store) error {
	// Seek to the beginning for replay.
	if _, err := a.file.Seek(0, 0); err != nil {
		return fmt.Errorf("aof seek: %w", err)
	}

	r := bufio.NewReader(a.file)
	replayed := 0
	for {
		parts, err := readCommand(r)
		if err != nil {
			// io.EOF is the normal end of the file.
			break
		}
		if len(parts) == 0 {
			continue
		}
		// Replay: run the command but discard the response.
		dispatch(parts, store)
		replayed++
	}
	log.Printf("AOF replay: %d commands", replayed)

	// Seek back to the end so subsequent writes append correctly.
	if _, err := a.file.Seek(0, 2); err != nil {
		return fmt.Errorf("aof seek to end: %w", err)
	}
	return nil
}
```

Key design points:
- `O_APPEND` tells the OS to seek to the end before every write, even if another process has extended the file. This is what makes appends safe without a seek before each write.
- `bufio.Writer` batches small writes to reduce system call overhead. `Flush()` is called after each command to ensure it is not stuck in the buffer when the process dies.
- `a.file.Sync()` (fsync) forces the OS to write its buffer to durable storage. This is expensive (~1-10 ms per call on spinning disk). Omitting it risks losing the last few commands on a hard crash (power loss), but survives normal process kills.
- `Replay` calls `dispatch` which modifies the store; it does not write to the AOF again (no double-logging).

---

## Sub-step C — Integrate AOF into main.go

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

	aof, err := OpenAOF(aofPath)
	if err != nil {
		log.Fatalf("failed to open AOF: %v", err)
	}
	defer aof.Close()

	// Restore state from the AOF before accepting connections.
	if err := aof.Replay(store); err != nil {
		log.Fatalf("AOF replay failed: %v", err)
	}

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
		go handleConn(conn, store, aof)
	}
}

func handleConn(conn net.Conn, store *Store, aof *AOF) {
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

		// Log write commands to the AOF.
		if isWriteCommand(parts[0]) {
			aof.Write(parts)
		}
	}
}
```

---

## Sub-step D — Identify write commands

Add a helper to `handler.go` (or a new `commands.go`):

```go
// isWriteCommand returns true for commands that mutate state and must be
// written to the AOF.
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
```

Read-only commands (`GET`, `TTL`, `EXISTS`, `KEYS`, `LRANGE`, `LLEN`, `PING`, `ECHO`) are never written to the AOF because replaying them would have no effect on state.

---

## Sub-step E — Test persistence

```bash
go run .
```

```bash
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 SET counter 0
redis-cli -p 6379 INCR counter
redis-cli -p 6379 RPUSH mylist x y z
```

Inspect the AOF:

```bash
cat aof/aof.log
```

You should see raw RESP for each write command.

Now kill the server with `Ctrl+C` and restart:

```bash
go run .
```

```bash
redis-cli -p 6379 GET foo
# "bar"  — survived restart

redis-cli -p 6379 GET counter
# "1"

redis-cli -p 6379 LRANGE mylist 0 -1
# 1) "x"  2) "y"  3) "z"
```

---

## Sub-step F — fsync policy trade-offs

| Policy | Durability | Performance |
|---|---|---|
| fsync every write | At most 0 commands lost on crash | Slowest (~1 write/ms) |
| fsync every second | At most ~1 second of commands lost | Good throughput |
| No fsync (OS decides) | At most OS buffer worth of commands lost | Fastest |

Real Redis defaults to `appendfsync everysec`. For this project, flushing the `bufio.Writer` after each command (without explicit fsync) is a reasonable default.

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `os.OpenFile` with `O_APPEND\|O_CREATE` | `OpenAOF` — safe append semantics |
| `bufio.Writer` with explicit `Flush` | Batching writes while ensuring durability |
| `file.Sync()` (fsync) | `AOF.Sync` — flushing OS buffer to disk |
| AOF replay = re-parsing + re-dispatching | `AOF.Replay` |
| `file.Seek(0, 0)` and `file.Seek(0, 2)` | Rewinding for replay then returning to end |
| Write command classification | `isWriteCommand` — what gets logged |
| Snapshot + WAL pattern | Groundwork for Stage 9's RDB |

---

## Checklist before moving to Stage 9

- [ ] `SET foo bar` followed by kill + restart restores `GET foo` → `bar`
- [ ] `RPUSH mylist x y z` survives restart with all elements intact
- [ ] `aof.log` is valid RESP that you can read with `cat`
- [ ] `INCR counter` replayed correctly (counter value matches)
- [ ] Starting the server with a pre-existing `aof.log` prints the replay count
- [ ] Read-only commands do not add entries to `aof.log`
- [ ] `go run -race .` shows no data races
---
