---
# Stage 1 — Project setup + TCP echo server

In this stage you bootstrap the Go module and write the first working server: a raw TCP echo server listening on port 6379 (the Redis default). Every byte sent to the server is echoed back. This verifies your network plumbing before you add any protocol logic.

---

## Sub-step A — Initialize the module

Create the project directory and initialize the Go module.

```bash
mkdir mykvstore
cd mykvstore
go mod init mykvstore
```

This creates `go.mod`. You do not need any external dependencies for this project — the Go standard library covers everything.

---

## Sub-step B — Write main.go with a TCP listener

Create `main.go`:

```go
package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
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
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	// Echo everything back to the client.
	if _, err := io.Copy(conn, conn); err != nil {
		log.Printf("connection error: %v", err)
	}
}
```

`net.Listen` binds the TCP socket. `ln.Accept()` blocks until a client connects and returns a `net.Conn` — a bidirectional stream. Each connection is handed off to a goroutine so the main loop can immediately accept the next client. `io.Copy(dst, src)` reads from `conn` and writes back to `conn` until EOF or an error, which is exactly echo behavior.

`defer conn.Close()` runs when `handleConn` returns, ensuring the file descriptor is released even if an error path is hit mid-function.

---

## Sub-step C — Test with netcat

In one terminal:

```bash
go run .
```

In a second terminal:

```bash
nc localhost 6379
```

Type any text and press Enter. The server echoes it back verbatim. Press `Ctrl+C` to close the client; the server goroutine exits cleanly and the main loop keeps accepting new connections.

---

## Sub-step D — Verify concurrent connections

Open two netcat sessions simultaneously. Each gets its own goroutine and its own echo stream — they do not interfere. This is the goroutine-per-connection model Redis itself used in its early single-threaded design (though Redis is single-threaded for command processing, the analogy is useful here).

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `net.Listen` + `net.Accept` | Main accept loop in `main()` |
| `net.Conn` as an `io.ReadWriter` | Passed to `io.Copy` for echo |
| Goroutine per connection | `go handleConn(conn)` |
| `defer conn.Close()` | Guaranteed cleanup in `handleConn` |
| `io.Copy` for bidirectional streaming | Echo implementation |
| `log.Fatalf` vs `log.Printf` | Fatal on bind failure, soft log on per-connection errors |

---

## Checklist before moving to Stage 2

- [ ] `go run .` starts without errors and prints `mykvstore listening on :6379`
- [ ] `nc localhost 6379` connects successfully
- [ ] Typing `hello` and pressing Enter returns `hello` from the server
- [ ] Opening two `nc` sessions at once works independently
- [ ] Closing `nc` with `Ctrl+C` does not crash the server
- [ ] `go build .` produces a binary with no compiler errors or warnings
---
