# Stage 6 — More commands: EXISTS, KEYS, INCR, APPEND

In this stage you round out the string command set. `EXISTS` checks key presence, `KEYS` lists keys matching a glob pattern, `INCR` treats a value as an integer and increments it atomically, and `APPEND` concatenates a suffix to an existing value. Together these cover the most common operations Redis applications use beyond bare GET/SET.

---

## Sub-step A — EXISTS

`EXISTS` in Redis accepts one or more keys and returns the count of keys that exist. A key repeated N times is counted N times.

Add to `store.go`:

```go
// Exists returns true if the key exists and is not expired.
func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isExpiredLocked(key) {
		return false
	}
	_, ok := s.data[key]
	return ok
}
```

Add to `handler.go` dispatch:

```go
case "EXISTS":
	if len(args) == 0 {
		return encodeError("wrong number of arguments for 'exists' command")
	}
	var count int64
	for _, key := range args {
		if store.Exists(key) {
			count++
		}
	}
	return encodeInteger(count)
```

---

## Sub-step B — KEYS with basic glob matching

`KEYS pattern` returns all keys matching a glob. In production Redis you would never call `KEYS` in a hot path (it blocks and scans the entire keyspace), but it is useful for debugging and scripting.

For now, support three glob patterns:
- `*` — match everything
- `prefix*` — match keys starting with prefix
- `*suffix` — match keys ending with suffix
- `*substr*` — match keys containing substr (handled as a single `strings.Contains`)

Add to `store.go`:

```go
// Keys returns all non-expired keys matching the given glob pattern.
// Supported wildcards: * (any sequence of characters). No ? or [] support.
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for key := range s.data {
		if s.isExpiredLocked(key) {
			continue
		}
		if matchGlob(pattern, key) {
			result = append(result, key)
		}
	}
	return result
}

// matchGlob matches key against a simple glob pattern that may contain
// one or more '*' wildcards. No '?' or character class support.
func matchGlob(pattern, key string) bool {
	// Fast paths.
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == key
	}

	// Split on '*' and verify the parts appear in order in key.
	parts := strings.Split(pattern, "*")
	pos := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		idx := strings.Index(key[pos:], part)
		if idx == -1 {
			return false
		}
		// The first segment must be a prefix match.
		if i == 0 && idx != 0 {
			return false
		}
		pos += idx + len(part)
	}
	// If the pattern does not end with '*', the last segment must be a suffix.
	if !strings.HasSuffix(pattern, "*") {
		last := parts[len(parts)-1]
		if last != "" && !strings.HasSuffix(key, last) {
			return false
		}
	}
	return true
}
```

Add `"strings"` to `store.go` imports.

Add to `handler.go`:

```go
case "KEYS":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'keys' command")
	}
	keys := store.Keys(args[0])
	return encodeArray(keys)
```

---

## Sub-step C — INCR

`INCR` is the classic Redis atomic counter. It reads the current value, parses it as a 64-bit integer, increments by 1, stores the new value, and returns it — all under the write lock so no other goroutine can interleave.

Also implement `INCRBY`, `DECR`, and `DECRBY` while you are here; they share the same logic.

Add to `store.go`:

```go
// Incr atomically increments the integer value of key by delta.
// If the key does not exist it is treated as 0.
// Returns the new value or an error if the current value is not an integer.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var current int64
	if val, ok := s.data[key]; ok {
		var err error
		current, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}
	next := current + delta
	s.data[key] = strconv.FormatInt(next, 10)
	return next, nil
}
```

Add `"strconv"` and `"fmt"` to `store.go` imports.

Add to `handler.go`:

```go
case "INCR":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'incr' command")
	}
	n, err := store.Incr(args[0], 1)
	if err != nil {
		return encodeError(err.Error())
	}
	return encodeInteger(n)

case "INCRBY":
	if len(args) != 2 {
		return encodeError("wrong number of arguments for 'incrby' command")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return encodeError("value is not an integer or out of range")
	}
	n, err := store.Incr(args[0], delta)
	if err != nil {
		return encodeError(err.Error())
	}
	return encodeInteger(n)

case "DECR":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'decr' command")
	}
	n, err := store.Incr(args[0], -1)
	if err != nil {
		return encodeError(err.Error())
	}
	return encodeInteger(n)

case "DECRBY":
	if len(args) != 2 {
		return encodeError("wrong number of arguments for 'decrby' command")
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return encodeError("value is not an integer or out of range")
	}
	n, err := store.Incr(args[0], -delta)
	if err != nil {
		return encodeError(err.Error())
	}
	return encodeInteger(n)
```

---

## Sub-step D — APPEND

`APPEND key value` appends a string suffix to the existing value and returns the new length in bytes.

Add to `store.go`:

```go
// Append concatenates suffix to the value of key and returns the new length.
// If the key does not exist it is created with suffix as its value.
func (s *Store) Append(key, suffix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = s.data[key] + suffix
	return len(s.data[key])
}
```

Add to `handler.go`:

```go
case "APPEND":
	if len(args) != 2 {
		return encodeError("wrong number of arguments for 'append' command")
	}
	n := store.Append(args[0], args[1])
	return encodeInteger(int64(n))
```

---

## Sub-step E — Test with redis-cli

```bash
go run .
```

```bash
# EXISTS
redis-cli -p 6379 SET a 1
redis-cli -p 6379 EXISTS a
# (integer) 1
redis-cli -p 6379 EXISTS a b
# (integer) 1  (b does not exist)
redis-cli -p 6379 EXISTS a a
# (integer) 2  (counted twice)

# KEYS
redis-cli -p 6379 SET user:1 alice
redis-cli -p 6379 SET user:2 bob
redis-cli -p 6379 SET session:1 xyz
redis-cli -p 6379 KEYS "user:*"
# 1) "user:1"
# 2) "user:2"
redis-cli -p 6379 KEYS "*"
# all keys

# INCR
redis-cli -p 6379 SET counter 10
redis-cli -p 6379 INCR counter
# (integer) 11
redis-cli -p 6379 INCRBY counter 5
# (integer) 16
redis-cli -p 6379 DECR counter
# (integer) 15
redis-cli -p 6379 INCR nonexistent
# (integer) 1  (starts from 0)

# APPEND
redis-cli -p 6379 SET greeting "Hello"
redis-cli -p 6379 APPEND greeting ", World"
# (integer) 12
redis-cli -p 6379 GET greeting
# "Hello, World"
```

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `strconv.ParseInt` / `FormatInt` | `Incr` — reading and writing integer values |
| Atomic read-modify-write under mutex | `Incr` — prevents lost updates |
| `strings.Split` on glob wildcard | `matchGlob` pattern matching |
| `strings.HasPrefix`, `HasSuffix`, `Contains` | `matchGlob` sub-checks |
| RESP integer type (`:N\r\n`) | Return type for INCR, APPEND, EXISTS |
| RESP array (`*N\r\n...`) | KEYS response |
| Map iteration for KEYS | `Store.Keys` scans `s.data` |
| Redis KEYS performance caveat | Discussed in Sub-step B |

---

## Checklist before moving to Stage 7

- [ ] `EXISTS key` returns `1` for present keys and `0` for absent keys
- [ ] `EXISTS a a` returns `2` (duplicate key counted twice)
- [ ] `KEYS "user:*"` returns only keys starting with `user:`
- [ ] `KEYS "*"` returns all keys
- [ ] `INCR` on a missing key starts from 0 and returns 1
- [ ] `INCR` on a non-integer value returns an error
- [ ] `INCRBY`, `DECR`, `DECRBY` all work correctly
- [ ] `APPEND` returns the new byte length
- [ ] `go run -race .` shows no data races
---
