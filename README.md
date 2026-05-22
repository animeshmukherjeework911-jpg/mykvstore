# mykvstore

A Redis-compatible key-value store written in Go, built stage by stage following the `stages/` guides.

## Features (implemented)

| Stage | Commands |
|---|---|
| 3 | `PING`, `ECHO` — RESP protocol |
| 4 | `SET`, `GET`, `DEL` — in-memory store |
| 5 | `SET EX/PX`, `EXPIRE`, `TTL`, `PERSIST` — key expiry |
| 6 | `EXISTS`, `KEYS`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `APPEND` |

## Running the server

```bash
# Default port 6379
go run .

# Custom port
go run . -port 6399
```

Connect with redis-cli:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
```

## Tests

### Unit tests

Test `Store` methods directly (no server needed). Includes `-race` to verify concurrent safety.

```bash
# From the project root
go test -race -v ./...
```

### Black-box integration tests

Test the full TCP stack via `go-redis` — same surface as a real Redis client. `TestMain` builds and starts the server on port 6399 automatically.

```bash
# From the test/ directory
cd test
go test -v ./...
```

#### First time setup

The `test/` directory is a separate Go module with its own dependencies:

```bash
cd test
go mod download   # fetch go-redis (already recorded in go.sum)
go test -v ./...
```

## Project layout

```
mykvstore/
├── main.go          # TCP listener, connection loop, -port flag
├── resp.go          # RESP parser and encoders
├── handler.go       # Command dispatch
├── store.go         # Thread-safe in-memory store (sync.RWMutex)
├── store_test.go    # Unit tests for Store methods
├── stages/          # Stage-by-stage build guides
└── test/
    ├── go.mod           # Separate module (go-redis dependency)
    └── blackbox_test.go # Black-box integration tests via go-redis
```
