# Stage 3 — Full RESP parser: arrays and bulk strings

`redis-cli` does not send inline commands by default. It sends RESP arrays of bulk strings — the full wire format that every Redis client uses. In this stage you replace the `bufio.Scanner` line reader with a proper RESP parser that can handle the array format, and you add a RESP encoder for all response types. After this stage, `redis-cli` can connect and `PING` successfully.

---

## Sub-step A — Understand the RESP array format

A `SET foo bar` command sent by `redis-cli` arrives as:

```
*3\r\n
$3\r\n
SET\r\n
$3\r\n
foo\r\n
$3\r\n
bar\r\n
```

Breaking this down:
- `*3` — an array of 3 elements
- `$3` — next bulk string is 3 bytes long
- `SET` — the 3 bytes (the `\r\n` after the value is the framing terminator, not part of the value)
- `$3` / `foo` — second element
- `$3` / `bar` — third element

The `$N` length prefix is what makes this a framed protocol: the parser reads exactly N bytes for the value, so values can contain newlines, spaces, or binary data without ambiguity. This is the key advantage over inline commands.

---

## Sub-step B — Refactor into separate files

Split the project into three files for clarity:

- `main.go` — listener and connection dispatch
- `resp.go` — RESP parser and encoder
- `handler.go` — command dispatch (grows in later stages)

---

## Sub-step C — Implement resp.go

```go
package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// readCommand reads one RESP command from r and returns it as a slice of
// strings. It handles both the full array format (*N) and inline commands.
func readCommand(r *bufio.Reader) ([]string, error) {
	// Peek at the first byte to decide which format we have.
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == '*' {
		return readArray(r)
	}

	// Inline command: put the byte back conceptually by reading the rest of
	// the line and prepending b.
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}
	line = string(b) + line
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, nil
	}
	return strings.Fields(line), nil
}

func readArray(r *bufio.Reader) ([]string, error) {
	// We have already consumed '*'; read the count.
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		s, err := readBulkString(r)
		if err != nil {
			return nil, err
		}
		parts = append(parts, s)
	}
	return parts, nil
}

func readBulkString(r *bufio.Reader) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	if b != '$' {
		return "", fmt.Errorf("expected '$', got %q", b)
	}

	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	length, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %w", err)
	}
	if length < 0 {
		// Null bulk string — treated as empty here; GET returns this for missing keys.
		return "", nil
	}

	// Read exactly length bytes plus the trailing \r\n.
	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:length]), nil
}

// --- Encoders ---

func encodeSimpleString(s string) string {
	return "+" + s + "\r\n"
}

func encodeError(msg string) string {
	return "-ERR " + msg + "\r\n"
}

func encodeInteger(n int64) string {
	return fmt.Sprintf(":%d\r\n", n)
}

func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func encodeNull() string {
	return "$-1\r\n"
}

func encodeArray(items []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(items))
	for _, item := range items {
		sb.WriteString(encodeBulkString(item))
	}
	return sb.String()
}
```

Key details:
- `bufio.Reader.ReadString('\n')` reads up to and including the delimiter, so the `\r\n` is included in the returned string and must be stripped.
- `io.ReadFull` reads exactly N bytes, blocking until all arrive. This is safer than a single `Read` call which may return fewer bytes than requested on a network connection.
- `strconv.Atoi` converts the ASCII length prefix to an integer.

---

## Sub-step D — Update main.go and handler.go

`main.go`:

```go
package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
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
	r := bufio.NewReader(conn)

	for {
		parts, err := readCommand(r)
		if err != nil {
			// EOF is normal (client disconnected).
			return
		}
		if len(parts) == 0 {
			continue
		}

		response := dispatch(parts)
		conn.Write([]byte(response))
	}
}
```

`handler.go`:

```go
package main

import (
	"fmt"
	"strings"
)

func dispatch(parts []string) string {
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
	case "COMMAND":
		// redis-cli sends COMMAND DOCS on startup; return an empty array.
		return "*0\r\n"
	default:
		return encodeError(fmt.Sprintf("unknown command '%s'", parts[0]))
	}
}
```

The `COMMAND` case is important: `redis-cli` sends `COMMAND DOCS` immediately after connecting to discover server capabilities. Returning an empty array satisfies it without crashing.

---

## Sub-step E — Test with redis-cli

```bash
go run .
```

In another terminal:

```bash
redis-cli -p 6379 PING
```

Expected: `PONG`

```bash
redis-cli -p 6379 ECHO "hello world"
```

Expected: `hello world`

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| RESP wire format (`+`, `-`, `:`, `$`, `*`) | All encoder functions in `resp.go` |
| `bufio.Reader.ReadString('\n')` | Reading length-prefixed lines |
| `strconv.Atoi` for length parsing | `readArray`, `readBulkString` |
| `io.ReadFull` for exact byte reads | Reading bulk string bodies |
| Framing vs delimiters | Why `$N` is better than newline-scanning for binary safety |
| `strings.Builder` for efficient concatenation | `encodeArray` |
| Handling `COMMAND DOCS` | Compatibility with `redis-cli` startup handshake |

---

## Checklist before moving to Stage 4

- [ ] `redis-cli -p 6379 PING` returns `PONG`
- [ ] `redis-cli -p 6379 ECHO "hello"` returns `hello`
- [ ] `redis-cli -p 6379 UNKNOWNCMD` returns an error starting with `(error) ERR`
- [ ] Inline commands via `nc localhost 6379` still work
- [ ] `go vet ./...` is clean
- [ ] No panics when a client disconnects mid-command
---
