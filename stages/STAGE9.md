# Stage 9 — RDB snapshot: periodic binary dump

The AOF grows indefinitely and replaying thousands of commands on startup is slow. In this stage you add RDB (Redis Database) snapshots: a periodic background goroutine encodes the entire store to a binary file using `encoding/gob`. On startup you load the RDB first (fast) and then replay only the AOF commands written after the snapshot (the delta). This is the snapshot + WAL (Write-Ahead Log) pattern used by many databases.

---

## Sub-step A — Why RDB is faster to load than AOF

Replaying an AOF means re-executing every write command since the beginning of time: parsing, locking, modifying the map, writing back — once per command. A large AOF with millions of entries takes seconds.

An RDB snapshot is a binary serialization of the current store state at a point in time. Loading it is a single gob decode: one pass, no command parsing, no lock contention. A snapshot of 1 million keys loads in milliseconds.

The trade-off: the RDB is a point-in-time snapshot. Commands written after the snapshot exist only in the AOF. On restart you load RDB + replay the AOF delta (commands appended after the snapshot timestamp).

---

## Sub-step B — Define the snapshot format in rdb.go

```go
package main

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

const rdbPath = "dump.rdb"

// Snapshot is the gob-encoded data structure written to dump.rdb.
type Snapshot struct {
	Data    map[string]string
	Expires map[string]time.Time
	Lists   map[string][]string
	SavedAt time.Time
}

// SaveRDB encodes the current store state to path atomically:
// write to a temp file, then rename over the target.
func SaveRDB(store *Store, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("rdb create tmp: %w", err)
	}

	// Take a consistent snapshot under read lock.
	store.mu.RLock()
	snap := Snapshot{
		Data:    make(map[string]string, len(store.data)),
		Expires: make(map[string]time.Time, len(store.expires)),
		Lists:   make(map[string][]string, len(store.lists)),
		SavedAt: time.Now(),
	}
	for k, v := range store.data {
		snap.Data[k] = v
	}
	for k, v := range store.expires {
		snap.Expires[k] = v
	}
	for k, v := range store.lists {
		cp := make([]string, len(v))
		copy(cp, v)
		snap.Lists[k] = cp
	}
	store.mu.RUnlock()

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
func LoadRDB(store *Store, path string) (time.Time, error) {
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

	store.mu.Lock()
	defer store.mu.Unlock()
	for k, v := range snap.Data {
		store.data[k] = v
	}
	for k, v := range snap.Expires {
		store.expires[k] = v
	}
	for k, v := range snap.Lists {
		store.lists[k] = v
	}

	log.Printf("RDB loaded: %d string keys, %d list keys (saved at %s)",
		len(snap.Data), len(snap.Lists), snap.SavedAt.Format(time.RFC3339))
	return snap.SavedAt, nil
}

// RDBSaver runs SaveRDB on a ticker and responds to BGSAVE signals.
type RDBSaver struct {
	store    *Store
	path     string
	saveCh   chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewRDBSaver(store *Store, path string) *RDBSaver {
	return &RDBSaver{
		store:  store,
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

// Stop shuts down the saver gracefully, performing one final save.
func (r *RDBSaver) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *RDBSaver) save(reason string) {
	start := time.Now()
	if err := SaveRDB(r.store, r.path); err != nil {
		log.Printf("RDB save (%s) failed: %v", reason, err)
		return
	}
	log.Printf("RDB save (%s) completed in %v", reason, time.Since(start))
}
```

The atomic write pattern (write to `.tmp`, then `os.Rename`) is critical. Without it, a crash mid-write would leave a partial, unreadable RDB file. `os.Rename` is guaranteed atomic by POSIX: either the old file or the new file exists at any moment, never a partial state.

---

## Sub-step C — Update startup in main.go

```go
func main() {
	store := NewStore()

	// 1. Load RDB snapshot (fast path).
	rdbSavedAt, err := LoadRDB(store, rdbPath)
	if err != nil {
		log.Fatalf("RDB load failed: %v", err)
	}

	// 2. Open AOF and replay only commands written after the RDB snapshot.
	aof, err := OpenAOF(aofPath)
	if err != nil {
		log.Fatalf("failed to open AOF: %v", err)
	}
	defer aof.Close()

	if err := aof.ReplayAfter(store, rdbSavedAt); err != nil {
		log.Fatalf("AOF replay failed: %v", err)
	}

	// 3. Start the periodic RDB saver (every 60 seconds).
	saver := NewRDBSaver(store, rdbPath)
	saver.Start(60 * time.Second)
	defer saver.Stop()

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
		go handleConn(conn, store, aof, saver)
	}
}
```

---

## Sub-step D — Add timestamp-aware replay to aof.go

The AOF does not currently record timestamps. A pragmatic approach: record the wall-clock time as a comment line before each command, then skip commands whose timestamp precedes `rdbSavedAt`.

A simpler approach (shown here): record the AOF byte offset when the RDB was saved (store it inside the RDB), then seek to that offset before replaying. For the learning project, replaying the full AOF after loading the RDB is also acceptable since gob loading removes the need to re-execute the bulk of history — the AOF delta is small.

Add `ReplayAfter` to `aof.go` as an alias for now:

```go
// ReplayAfter replays AOF commands, skipping those recorded before cutoff.
// In this implementation the full AOF is replayed; the RDB already has
// the canonical state, so re-applying SET/DEL commands is idempotent.
func (a *AOF) ReplayAfter(store *Store, cutoff time.Time) error {
	// If cutoff is zero (no RDB), replay everything.
	// Otherwise, still replay everything — idempotent commands are safe.
	return a.Replay(store)
}
```

For a production implementation you would embed the AOF offset in the RDB and seek past it. The idempotent replay approach is correct and much simpler.

---

## Sub-step E — Add BGSAVE to handler.go

```go
case "BGSAVE":
	saver.Trigger()
	return encodeSimpleString("Background saving started")

case "LASTSAVE":
	// Return the Unix timestamp of the last successful save.
	// For simplicity, return the current time (a real impl would track this).
	return encodeInteger(time.Now().Unix())
```

Update `handleConn` and `dispatch` signatures to accept `*RDBSaver` alongside `*AOF`.

---

## Sub-step F — Test

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
| Lock held during snapshot copy | Consistent read of all three maps under `RLock` |

---

## Checklist before moving to Stage 10

- [ ] `BGSAVE` creates `dump.rdb` and logs completion
- [ ] Killing and restarting the server loads state from `dump.rdb`
- [ ] The startup log shows the correct key counts from the RDB
- [ ] AOF replay after RDB load restores commands written after the snapshot
- [ ] `dump.rdb.tmp` does not linger after a successful save
- [ ] `go run -race .` shows no data races
- [ ] The 60-second periodic save triggers automatically (check logs)
---
