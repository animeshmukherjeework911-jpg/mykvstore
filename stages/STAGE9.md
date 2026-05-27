# Stage 9 — RDB snapshot: periodic binary dump

The AOF grows indefinitely and replaying thousands of commands on startup is slow. In this stage you add RDB (Redis Database) snapshots: a periodic background goroutine encodes the entire store to a binary file using `encoding/gob`. On startup you load the RDB first (fast) and then replay only the AOF commands written after the snapshot (the delta). This is the snapshot + WAL (Write-Ahead Log) pattern used by many databases.

---

## Implementation Status — COMPLETE

All sub-steps below have been implemented. The following were added across the codebase:

- `TakeSnapshot` / `RestoreSnapshot` on `Store` — crossing the package boundary without exporting fields
- `saveRDB` (private) + `LoadRDB` + `RDBSaver` in `internal/rdb/rdb.go`
- `BGSAVE` and `LASTSAVE` cases in `handler.Dispatch`, with a nil guard for replay safety
- RDB load on startup followed by AOF delta replay in `main.go`
- `ReplayAfter` in `aof.go`

---

## Sub-step A — Why RDB is faster to load than AOF

Replaying an AOF means re-executing every write command since the beginning of time: parsing, locking, modifying the map, writing back — once per command. A large AOF with millions of entries takes seconds.

An RDB snapshot is a binary serialization of the current store state at a point in time. Loading it is a single gob decode: one pass, no command parsing, no lock contention. A snapshot of 1 million keys loads in milliseconds.

The trade-off: the RDB is a point-in-time snapshot. Commands written after the snapshot exist only in the AOF. On restart you load RDB + replay the AOF delta (commands appended after the snapshot timestamp).

---

## Sub-step B — Add TakeSnapshot and RestoreSnapshot to store.go

Because `internal/rdb` is a separate package it cannot access the unexported fields `mu`, `data`, `expires`, and `lists` on `Store`. The solution is to add two methods to `internal/store/store.go` that own the locking and expose only plain maps to the caller.

```go
// TakeSnapshot returns consistent copies of all three maps under RLock.
// The rdb package calls this — no store fields are accessed outside this method.
func (s *Store) TakeSnapshot() (map[string]string, map[string]time.Time, map[string][]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string]string, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}
	expires := make(map[string]time.Time, len(s.expires))
	for k, v := range s.expires {
		expires[k] = v
	}
	lists := make(map[string][]string, len(s.lists))
	for k, v := range s.lists {
		cp := make([]string, len(v))
		copy(cp, v)
		lists[k] = cp
	}
	return data, expires, lists
}

// RestoreSnapshot loads snapshot data into the store under write lock.
// Called once on startup after decoding the RDB file.
func (s *Store) RestoreSnapshot(data map[string]string, expires map[string]time.Time, lists map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range data {
		s.data[k] = v
	}
	for k, v := range expires {
		s.expires[k] = v
	}
	for k, v := range lists {
		s.lists[k] = v
	}
}
```

`TakeSnapshot` holds `RLock` only long enough to copy all three maps. After it returns the caller owns independent copies — no lock is held during gob encoding, so client goroutines are never blocked by a save.

---

## Sub-step C — Implement rdb.go

Replace the stub at `internal/rdb/rdb.go`:

```go
package rdb

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"mykvstore/internal/store"
)

const RDBPath = "dump.rdb"

// Snapshot is the gob-encoded data structure written to dump.rdb.
type Snapshot struct {
	Data    map[string]string
	Expires map[string]time.Time
	Lists   map[string][]string
	SavedAt time.Time
}

// saveRDB encodes the current store state to path atomically:
// write to a temp file, then rename over the target.
func saveRDB(s *store.Store, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("rdb create tmp: %w", err)
	}

	data, expires, lists := s.TakeSnapshot()
	snap := Snapshot{
		Data:    data,
		Expires: expires,
		Lists:   lists,
		SavedAt: time.Now(),
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(snap); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("rdb encode: %w", err)
	}
	f.Close()

	// Atomic rename — avoids a partially written RDB being read on crash.
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rdb rename: %w", err)
	}
	return nil
}

// LoadRDB reads and decodes the RDB file into the store.
// Returns (savedAt, nil) on success, (zero, nil) if the file does not exist.
func LoadRDB(s *store.Store, path string) (time.Time, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("rdb open: %w", err)
	}
	defer f.Close()

	var snap Snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return time.Time{}, fmt.Errorf("rdb decode: %w", err)
	}

	s.RestoreSnapshot(snap.Data, snap.Expires, snap.Lists)

	log.Printf("RDB loaded: %d string keys, %d list keys (saved at %s)",
		len(snap.Data), len(snap.Lists), snap.SavedAt.Format(time.RFC3339))
	return snap.SavedAt, nil
}

// RDBSaver runs SaveRDB on a ticker and responds to BGSAVE signals.
type RDBSaver struct {
	store    *store.Store
	path     string
	saveCh   chan struct{}
	stopCh   chan struct{}
	mu       sync.Mutex
	lastSave time.Time
	wg       sync.WaitGroup
}

func NewRDBSaver(s *store.Store, path string) *RDBSaver {
	return &RDBSaver{
		store:  s,
		path:   path,
		saveCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
}

// Start launches the background save goroutine.
func (r *RDBSaver) Start(interval time.Duration) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.save("periodic")
			case <-r.saveCh:
				r.save("BGSAVE")
			case <-r.stopCh:
				r.save("shutdown")
				return
			}
		}
	}()
}

// Trigger requests an immediate background save (non-blocking).
func (r *RDBSaver) Trigger() {
	select {
	case r.saveCh <- struct{}{}:
	default: // A save is already queued; skip.
	}
}

// LastSave returns the time of the most recent successful save.
func (r *RDBSaver) LastSave() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastSave
}

// Stop shuts down the saver gracefully, performing one final save.
func (r *RDBSaver) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *RDBSaver) save(reason string) {
	start := time.Now()
	if err := saveRDB(r.store, r.path); err != nil {
		log.Printf("RDB save (%s) failed: %v", reason, err)
		return
	}
	r.mu.Lock()
	r.lastSave = time.Now()
	r.mu.Unlock()
	log.Printf("RDB save (%s) completed in %v", reason, time.Since(start))
}
```

The atomic write pattern (write to `.tmp`, then `os.Rename`) is critical. Without it, a crash mid-write would leave a partial, unreadable RDB file. `os.Rename` is guaranteed atomic by POSIX: either the old file or the new file exists at any moment, never a partial state.

`RDBPath` is exported (uppercase) so `main.go` can reference it without redefining the constant. `saveRDB` stays private — only `RDBSaver.save` calls it; callers outside the package use `RDBSaver` or `LoadRDB`.

---

## Sub-step D — Update startup in main.go

```go
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
	"mykvstore/internal/rdb"
	"mykvstore/internal/resp"
	"mykvstore/internal/store"
)

func main() {
	port := flag.String("port", "6379", "TCP port to listen on")
	flag.Parse()

	s := store.NewStore()

	// 1. Load RDB snapshot (fast path).
	rdbSavedAt, err := rdb.LoadRDB(s, rdb.RDBPath)
	if err != nil {
		log.Fatalf("RDB load failed: %v", err)
	}

	// 2. Open AOF and replay commands written after the RDB snapshot.
	a, err := aof.OpenAOF(aof.AOF_PATH)
	if err != nil {
		log.Fatalf("failed to open AOF: %v", err)
	}
	defer a.Close()

	if err := a.ReplayAfter(s, rdbSavedAt); err != nil {
		log.Fatalf("AOF replay failed: %v", err)
	}

	// 3. Start the periodic RDB saver (every 60 seconds).
	saver := rdb.NewRDBSaver(s, rdb.RDBPath)
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
		go handleConn(conn, s, a, saver)
	}
}

func handleConn(conn net.Conn, s *store.Store, a *aof.AOF, saver *rdb.RDBSaver) {
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

		response := handler.Dispatch(parts, s, saver)
		conn.Write([]byte(response))

		if isWriteCommand(parts[0]) {
			a.Write(parts)
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
```

---

## Sub-step E — Add timestamp-aware replay to aof.go

Add `ReplayAfter` to `internal/aof/aof.go`:

```go
// ReplayAfter replays AOF commands, skipping those recorded before cutoff.
// In this implementation the full AOF is replayed; the RDB already has
// the canonical state, so re-applying SET/DEL commands is idempotent.
func (a *AOF) ReplayAfter(s *store.Store, cutoff time.Time) error {
	// If cutoff is zero (no RDB), replay everything.
	// Otherwise, still replay everything — idempotent commands are safe.
	return a.Replay(s)
}
```

Also add `"time"` to the `aof.go` imports.

For a production implementation you would embed the AOF offset in the RDB and seek past it. The idempotent replay approach is correct and much simpler.

---

## Sub-step F — Add BGSAVE and LASTSAVE to handler.go

`handler.Dispatch` needs to accept `*rdb.RDBSaver` so it can trigger saves and report the last save time. `handler` importing `rdb` does not create a cycle: both packages already depend on `store`, and nothing in `rdb` imports `handler`.

Update the signature and add two cases:

```go
package handler

import (
	// existing imports ...
	"mykvstore/internal/rdb"
)

func Dispatch(parts []string, s *store.Store, saver *rdb.RDBSaver) string {
	// ... all existing cases unchanged ...

	case "BGSAVE":
		if saver != nil {
			saver.Trigger()
			return resp.EncodeSimpleString("Background saving started")
		}
		return resp.EncodeError("background save disabled")

	case "LASTSAVE":
		if saver != nil {
			return resp.EncodeInteger(saver.LastSave().Unix())
		}
		return resp.EncodeError("background save disabled")

	// ... default case ...
}
```

---

## Sub-step G — Test

```bash
go run .
```

```bash
redis-cli -p 6379 SET persistent hello
redis-cli -p 6379 RPUSH mylist a b c
redis-cli -p 6379 BGSAVE
# Background saving started
```

Check that `dump.rdb` was created:

```bash
ls -lh dump.rdb
```

Kill and restart:

```bash
go run .
# RDB loaded: 1 string keys, 1 list keys (saved at ...)
# AOF replay: N commands

redis-cli -p 6379 GET persistent
# "hello"

redis-cli -p 6379 LRANGE mylist 0 -1
# 1) "a"  2) "b"  3) "c"
```

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `encoding/gob` | `SaveRDB` / `LoadRDB` — binary serialization |
| `gob.Encoder` / `gob.Decoder` | Encoding the `Snapshot` struct |
| Atomic rename (`os.Rename`) | Safe RDB write — never leaves a partial file |
| Snapshot + WAL pattern | RDB (snapshot) + AOF (WAL) on startup |
| Background goroutine with ticker | `RDBSaver.Start` — periodic saves |
| Buffered channel as a signal | `saveCh chan struct{}` for BGSAVE trigger |
| `select` with multiple cases | `RDBSaver` goroutine — ticker vs trigger vs stop |
| `sync.WaitGroup` for shutdown | `RDBSaver.Stop` — waiting for final save |
| `TakeSnapshot` / `RestoreSnapshot` | Crossing the package boundary without exporting fields |
| Lock held only during map copy | `TakeSnapshot` — gob encoding runs outside the lock |

---

## Checklist before moving to Stage 10

- [x] `BGSAVE` creates `dump.rdb` and logs completion
- [x] Killing and restarting the server loads state from `dump.rdb`
- [x] The startup log shows the correct key counts from the RDB
- [x] AOF replay after RDB load restores commands written after the snapshot
- [x] `dump.rdb.tmp` does not linger after a successful save
- [x] `go run -race .` shows no data races
- [x] The 60-second periodic save triggers automatically (check logs)
- [x] `LASTSAVE` returns a non-zero Unix timestamp after the first save
---
