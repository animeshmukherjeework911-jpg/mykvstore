---
# Stage 7 — List data structure: LPUSH, RPUSH, LPOP, RPOP, LRANGE

Redis is not just a string store — it supports multiple data types. In this stage you add list support: a separate map in `Store` holds `[]string` slices, and you implement the five core list commands. You also learn how Go slices behave as a deque and how to handle the type mismatch error when a string key and a list key collide.

---

## Sub-step A — Design decisions

Go does not have a built-in deque. You will use a `[]string` slice:
- **RPUSH** — `append(slice, elem)`. O(amortized 1).
- **LPUSH** — prepend: `slice = append([]string{elem}, slice...)`. O(n) because all existing elements shift. For a production system you would use a linked list or a ring buffer. For a learning project, slices are fine.
- **RPOP** — take and remove the last element: `slice[len-1]`, then `slice[:len-1]`.
- **LPOP** — take and remove the first element: `slice[0]`, then `slice[1:]`. Note that `slice[1:]` does not free the underlying array — Redis uses a linked list to avoid this. For now it is acceptable.

Type safety: a key can hold either a string value or a list, but not both. If a `GET` is called on a list key (or `LPUSH` on a string key), return a WRONGTYPE error, which is the exact error Redis produces.

---

## Sub-step B — Extend Store with a list map

Update `store.go` to add `lists map[string][]string`. The `data` map remains for string keys. Add type-checking helpers:

```go
type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time
	lists   map[string][]string
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
		lists:   make(map[string][]string),
	}
}
```

Add list methods to `store.go`:

```go
const wrongTypeErr = "WRONGTYPE Operation against a key holding the wrong kind of value"

// LPush prepends one or more values to the list at key.
// Returns the new length of the list after the push.
// Returns an error string if the key holds a non-list value.
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}
	// LPUSH prepends in order: LPUSH k a b c → list is [c, b, a].
	for _, v := range values {
		s.lists[key] = append([]string{v}, s.lists[key]...)
	}
	return len(s.lists[key]), nil
}

// RPush appends one or more values to the list at key.
// Returns the new length of the list.
func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}
	s.lists[key] = append(s.lists[key], values...)
	return len(s.lists[key]), nil
}

// LPop removes and returns the first element of the list.
// Returns ("", false) if the list is empty or does not exist.
func (s *Store) LPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, isString := s.data[key]; isString {
		return "", false, fmt.Errorf(wrongTypeErr)
	}
	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}
	val := list[0]
	s.lists[key] = list[1:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key) // Remove the key when the list empties.
	}
	return val, true, nil
}

// RPop removes and returns the last element of the list.
func (s *Store) RPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, isString := s.data[key]; isString {
		return "", false, fmt.Errorf(wrongTypeErr)
	}
	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}
	n := len(list)
	val := list[n-1]
	s.lists[key] = list[:n-1]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return val, true, nil
}

// LLen returns the length of the list at key.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}
	return len(s.lists[key]), nil
}

// LRange returns the elements of the list between start and stop (inclusive).
// Negative indices count from the end: -1 is the last element.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, isString := s.data[key]; isString {
		return nil, fmt.Errorf(wrongTypeErr)
	}
	list := s.lists[key]
	n := len(list)
	if n == 0 {
		return []string{}, nil
	}

	// Resolve negative indices.
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	// Clamp to valid range.
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop {
		return []string{}, nil
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result, nil
}
```

---

## Sub-step C — Add list commands to handler.go

```go
case "LPUSH":
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'lpush' command")
	}
	n, err := store.LPush(args[0], args[1:]...)
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	return encodeInteger(int64(n))

case "RPUSH":
	if len(args) < 2 {
		return encodeError("wrong number of arguments for 'rpush' command")
	}
	n, err := store.RPush(args[0], args[1:]...)
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	return encodeInteger(int64(n))

case "LPOP":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'lpop' command")
	}
	val, ok, err := store.LPop(args[0])
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	if !ok {
		return encodeNull()
	}
	return encodeBulkString(val)

case "RPOP":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'rpop' command")
	}
	val, ok, err := store.RPop(args[0])
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	if !ok {
		return encodeNull()
	}
	return encodeBulkString(val)

case "LLEN":
	if len(args) != 1 {
		return encodeError("wrong number of arguments for 'llen' command")
	}
	n, err := store.LLen(args[0])
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	return encodeInteger(int64(n))

case "LRANGE":
	if len(args) != 3 {
		return encodeError("wrong number of arguments for 'lrange' command")
	}
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return encodeError("value is not an integer or out of range")
	}
	items, err := store.LRange(args[0], start, stop)
	if err != nil {
		return "-" + err.Error() + "\r\n"
	}
	return encodeArray(items)
```

Note the WRONGTYPE error uses a bare `-` prefix rather than `-ERR` — Redis outputs the WRONGTYPE string verbatim without the `ERR` prefix, and clients display it distinctly.

---

## Sub-step D — Test with redis-cli

```bash
go run .
```

```bash
redis-cli -p 6379 RPUSH mylist a b c
# (integer) 3

redis-cli -p 6379 LRANGE mylist 0 -1
# 1) "a"
# 2) "b"
# 3) "c"

redis-cli -p 6379 LPUSH mylist z
# (integer) 4

redis-cli -p 6379 LRANGE mylist 0 -1
# 1) "z"
# 2) "a"
# 3) "b"
# 4) "c"

redis-cli -p 6379 LPOP mylist
# "z"

redis-cli -p 6379 RPOP mylist
# "c"

redis-cli -p 6379 LLEN mylist
# (integer) 2

redis-cli -p 6379 LRANGE mylist 0 0
# 1) "a"

# Type error:
redis-cli -p 6379 SET strkey hello
redis-cli -p 6379 LPUSH strkey x
# (error) WRONGTYPE Operation against a key holding the wrong kind of value
```

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| Go slice as a deque | `LPush` (prepend), `RPush` (append), `LPop`, `RPop` |
| Prepend cost is O(n) | `LPush` — `append([]string{v}, slice...)` copies all elements |
| Negative index resolution | `LRange` — `n + index` for negative start/stop |
| Clamping slice indices | `LRange` — bounds checking before slicing |
| `copy` to avoid aliasing | `LRange` returns an independent copy of the sub-slice |
| WRONGTYPE error | Type-checking before every list operation |
| Deleting empty list key | `LPop`/`RPop` — `delete(s.lists, key)` when length hits 0 |
| Multiple return values for errors | All list methods return `(value, ok, error)` |

---

## Checklist before moving to Stage 8

- [ ] `RPUSH mylist a b c` returns `(integer) 3`
- [ ] `LRANGE mylist 0 -1` returns all elements in insertion order
- [ ] `LPOP` returns and removes the first element
- [ ] `RPOP` returns and removes the last element
- [ ] `LRANGE` with negative indices works correctly (`-1` is last element)
- [ ] `LLEN` returns current length
- [ ] `LPOP` on an empty list returns `(nil)`
- [ ] `LPUSH` on a string key returns a WRONGTYPE error
- [ ] `go run -race .` shows no data races
---
