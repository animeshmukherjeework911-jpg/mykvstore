# mykvstore

A Redis-compatible key-value store written in Go, built stage by stage following the `stages/` guides.

## Features (implemented)

| Stage | Commands |
|---|---|
| 3 | `PING`, `ECHO` — RESP protocol |
| 4 | `SET`, `GET`, `DEL` — in-memory store |
| 5 | `SET EX/PX`, `EXPIRE`, `TTL`, `PERSIST` — key expiry |
| 6 | `EXISTS`, `KEYS`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `APPEND` |

## Setup

Clone and install dependencies:

```bash
git clone <repo-url>
cd mykvstore
```

Sync the workspace (resolves all three modules in one shot):

```bash
go work sync
```

Download blackbox test dependencies (go-redis):

```bash
cd test/blackbox
go mod download
cd ../..
```

Install `gotestsum` for pretty test output:

```bash
go install gotest.tools/gotestsum@latest
```

Verify the build:

```bash
go build .
```

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

Test `Store` methods directly against the internal package — no server needed. Includes `-race` to verify concurrent safety.

```bash
cd test/unit
gotestsum --format testname -- -race ./...
```

### Blackbox integration tests

Test the full TCP stack via `go-redis` — same surface as a real Redis client. `TestMain` builds and starts the server on port 6399 automatically.

```bash
cd test/blackbox
gotestsum --format testname ./...
```

### Without gotestsum

```bash
# Unit
cd test/unit
go test -race -v ./... | grep -E "^(=== RUN|--- PASS|--- FAIL|FAIL|ok)"

# Blackbox
cd test/blackbox
go test -v ./... | grep -E "^(=== RUN|--- PASS|--- FAIL|FAIL|ok)"
```

## Project layout

```
mykvstore/
├── main.go                      # TCP listener, connection loop, -port flag
├── go.mod                       # Root module — also declares gotestsum tool
├── go.work                      # Workspace — ties root, test/unit, test/blackbox together
├── internal/
│   ├── resp/
│   │   └── resp.go              # RESP parser and encoders
│   ├── handler/
│   │   └── handler.go           # Command dispatch
│   └── store/
│       └── store.go             # Thread-safe in-memory store (sync.RWMutex)
├── stages/                      # Stage-by-stage build guides
└── test/
    ├── unit/
    │   ├── go.mod               # module mykvstore/test/unit
    │   └── store_test.go        # Unit tests for Store — no server needed
    └── blackbox/
        ├── go.mod               # module mykvstore/test/blackbox — owns go-redis dep
        ├── go.sum
        └── blackbox_test.go     # Black-box integration tests via go-redis
```
