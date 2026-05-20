# Stage 5 — TTL and EXPIRE: time-based expiry

Redis is famous for its ability to automatically expire keys. In this stage you add expiry support: a second map stores the deadline for each key, `GET` checks whether the deadline has passed and returns null if so, and you implement the `EXPIRE`, `TTL`, and `PERSIST` commands. You also add the `EX` and `PX` options to `SET`.

---

## Sub-step A — Lazy expiry vs active expiry

There are two approaches to expiring keys:

**Lazy expiry**: Check the deadline only when the key is accessed. Keys that are never read stay in memory forever (or until the next access). Simple to implement.

**Active expiry**: A background goroutine periodically scans the keyspace and deletes expired entries, reclaiming memory proactively.

Redis uses both: lazy expiry on every access plus an active expiry loop that samples random keys 10 times per second. In this stage you implement lazy expiry. An optional active expiry goroutine is shown at the end.

---

## Sub-step B — Add expiry to store.go

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
	}
}

// isExpiredLocked checks expiry. Must be called with at least RLock held.
func (s *Store) isExpiredLocked(key string) bool {
	deadline, ok := s.expires[key]
	if !ok {
		return false
	}
	return time.Now().After(deadline)
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	delete(s.expires, key) // Clear any previous TTL.
}

// SetWithTTL stores key → value and sets an expiry duration.
func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.expires[key] = time.Now().Add(ttl)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isExpiredLocked(key) {
		return "", false
	}
	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
		delete(s.expires, key)
	}
	return ok
}

// Expire sets or replaces the TTL for an existing key.
// Returns true if the key exists and is not expired, false otherwise.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	if s.isExpiredLocked(key) {
		return false
	}
	s.expires[key] = time.Now().Add(ttl)
	return true
}

// TTL returns the remaining time-to-live for a key.
// Returns -1 if the key exists but has no expiry.
// Returns -2 if the key does not exist or has already expired.
func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.data[key]; !ok {
		return -2 * time.Second
	}
	if s.isExpiredLocked(key) {
		return -2 * time.Second
	}
	deadline, ok := s.expires[key]
	if !ok {
		return -1 * time.Second // No expiry set.
	}
	return time.Until(deadline)
}

// Persist removes the expiry from a key.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	_, had := s.expires[key]
	delete(s.expires, key)
	return had
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store(%d keys)", len(s.data))
}
```

The sentinel values `-1` and `-2` for `TTL` match Redis semantics exactly: `-1` means the key has no associated expiry, `-2` means the key does not exist.

---

## Sub-step C — Add active expiry (optional background goroutine)

Add this to `main.go` before the accept loop:

```go
go func() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		store.evictExpired()
	}
}()
```

Add `evictExpired` to `store.go`:

```go
// evictExpired deletes all expired keys. Called by the background goroutine.
func (s *Store) evictExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for key, deadline := range s.expires {
		if now.After(deadline) {
			delete(s.data, key)
			delete(s.expires, key)
		}
	}
}
```

This runs every 100 ms and reclaims memory from expired keys even if they are never accessed again.

---

## Sub-step D — Update the SET handler to support EX and PX

Update the `SET` case in `handler.go`:

```go
case "SET":
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'set' command")
	}
	key, value := args[0], args[1]
	opts := args[2:]

	// Parse optional EX/PX flags.
	var ttl time.Duration
	for i := 0; i < len(opts)-1; i++ {
		switch strings.ToUpper(opts[i]) {
		case "EX":
			n, err := strconv.Atoi(opts[i+1])
			if err != nil || n <= 0 {
				return encodeError("invalid expire time in 'set' command")
			}
			ttl = time.Duration(n) * time.Second
			i++
		case "PX":
			n, err := strconv.Atoi(opts[i+1])
			if err != nil || n <= 0 {
				return encodeError("invalid expire time in 'set' command")
			}
			ttl = time.Duration(n) * time.Millisecond
			i++
		}
	}

	if ttl > 0 {
		store.SetWithTTL(key, value, ttl)
	} else {
		store.Set(key, value)
	}
	return encodeSimpleString("OK")
```

Add these new commands to `dispatch`:

```go
case "EXPIRE":
	if len(args) != 2 {
		return encodeError("wrong number of arguments for 'expire' command")
	}
	n, err := strconv.Atoi(args[1])
	if err != nil {
		return encodeError("value is not an integer or out of range")
	}
	if store.Expire(args[0], time.Duration(n)*time.Second) {
		return encodeInteger(1)
	}
	return encodeInteger(0)

case "TTL":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'ttl' command")
	}
	d := store.TTL(args[0])
	return encodeInteger(int64(d.Seconds()))

case "PERSIST":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'persist' command")
	}
	if store.Persist(args[0]) {
		return encodeInteger(1)
	}
	return encodeInteger(0)
```

Add `"time"` and `"strconv"` to the imports in `handler.go`.

---

## Sub-step E — Test with redis-cli

```bash
go run .
```

```bash
redis-cli -p 6379 SET foo bar EX 2
# OK

redis-cli -p 6379 TTL foo
# (integer) 1  (or 2 depending on timing)

redis-cli -p 6379 GET foo
# "bar"

sleep 3

redis-cli -p 6379 GET foo
# (nil)

redis-cli -p 6379 TTL foo
# (integer) -2

redis-cli -p 6379 SET counter 42
redis-cli -p 6379 EXPIRE counter 30
# (integer) 1
redis-cli -p 6379 TTL counter
# (integer) ~29

redis-cli -p 6379 PERSIST counter
# (integer) 1
redis-cli -p 6379 TTL counter
# (integer) -1
```

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `time.Time` as a deadline | `Store.expires` map |
| `time.Now().After(deadline)` | `isExpiredLocked` |
| `time.Duration` arithmetic | `SetWithTTL`, `Expire` |
| `time.Until` | Computing remaining TTL |
| Lazy expiry | Checking deadline on `Get` |
| Active expiry | Background goroutine calling `evictExpired` |
| `time.NewTicker` | Background eviction loop |
| Redis TTL sentinel values (-1, -2) | `Store.TTL` return conventions |
| `EX` / `PX` option parsing | `SET` handler |

---

## Checklist before moving to Stage 6

- [ ] `SET foo bar EX 2` followed by `GET foo` returns `bar` within 2 seconds
- [ ] `GET foo` after 2 seconds returns `(nil)`
- [ ] `TTL foo` returns a positive integer before expiry
- [ ] `TTL foo` returns `-2` after expiry or for missing keys
- [ ] `TTL` on a key with no expiry returns `-1`
- [ ] `PERSIST` removes expiry and subsequent `TTL` returns `-1`
- [ ] `EXPIRE nonexistent 10` returns `(integer) 0`
- [ ] `go run -race .` shows no data races
---
