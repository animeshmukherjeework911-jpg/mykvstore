---
# Stage 2 — RESP protocol: parse inline commands

The raw echo server from Stage 1 has no concept of commands. In this stage you add the first layer of the Redis Serialization Protocol (RESP): inline commands. An inline command is a single line of space-separated text ending in `\r\n`. You will parse `PING` and respond with `+PONG\r\n`, which is the first thing `redis-cli` sends when it connects.

---

## Sub-step A — Understand the RESP inline format

RESP supports two command styles. Inline commands are the simpler one: a client sends a line of text, the server splits it on whitespace to get the command name and arguments. For example:

```
PING\r\n
SET foo bar\r\n
```

The server's responses always use RESP encoding:
- Simple string: `+OK\r\n`
- Error: `-ERR some message\r\n`
- Integer: `:42\r\n`
- Bulk string: `$3\r\nfoo\r\n`
- Null: `$-1\r\n`

In Stage 3 you will implement the full array-based RESP parser that `redis-cli` uses by default. For now, inline is enough to test with `nc`.

---

## Sub-step B — Replace the echo handler with a line reader

Update `main.go` to read lines and dispatch commands:

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

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text() // strips the trailing \n; Text() also strips \r
		if line == "" {
			continue
		}
		parts := strings.Fields(line) // splits on any whitespace
		if len(parts) == 0 {
			continue
		}

		response := dispatch(parts)
		conn.Write([]byte(response))
	}

	if err := scanner.Err(); err != nil {
		log.Printf("scanner error: %v", err)
	}
}

func dispatch(parts []string) string {
	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	switch cmd {
	case "PING":
		if len(args) == 0 {
			return "+PONG\r\n"
		}
		// PING with a message echoes the message as a bulk string.
		msg := args[0]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
	case "ECHO":
		if len(args) == 0 {
			return "-ERR wrong number of arguments for 'echo' command\r\n"
		}
		msg := args[0]
		return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
	default:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", parts[0])
	}
}
```

`bufio.Scanner` wraps the `net.Conn` and buffers reads internally. Each call to `scanner.Scan()` reads up to the next newline. `scanner.Text()` returns the line without the trailing newline (and without `\r` if the line ended with `\r\n`). `strings.Fields` splits on any sequence of whitespace and trims leading/trailing space, which is more robust than `strings.Split(line, " ")`.

---

## Sub-step C — Test with netcat

```bash
go run .
```

In another terminal:

```bash
nc localhost 6379
PING
```

Expected response: `+PONG`

```bash
ECHO hello
```

Expected response: `$5` followed by `hello`.

```bash
SET foo bar
```

Expected response: `-ERR unknown command 'SET'` — that command comes in Stage 4.

---

## Sub-step D — Why Redis chose a text protocol

Redis uses a text-based protocol deliberately. Text protocols are:
- Human readable — you can debug with `nc` or `telnet` without special tools
- Easy to implement in any language without binary parsing libraries
- Simple to version and extend

The trade-off is slight verbosity compared to a binary protocol, but for an in-memory store where network I/O is rarely the bottleneck, this is a good trade.

---

## Sub-step E — Note on bufio.Scanner limits

The default `bufio.Scanner` buffer is 64 KB per line. For large values (e.g., storing a megabyte blob) this will be insufficient. In Stage 3 you will switch to `bufio.Reader` which lets you control read size precisely. For now, 64 KB is fine.

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| `bufio.Scanner` wrapping `net.Conn` | Reading lines from the TCP stream |
| `scanner.Text()` strips `\r\n` | Cleaner than manual trimming |
| `strings.Fields` | Splitting the inline command on whitespace |
| RESP simple string (`+...`) | `+PONG\r\n` response |
| RESP error (`-...`) | Unknown command response |
| RESP bulk string (`$N\r\n...\r\n`) | ECHO response |
| `strings.ToUpper` for case-insensitive dispatch | Command normalization |

---

## Checklist before moving to Stage 3

- [ ] `go run .` starts cleanly
- [ ] `nc localhost 6379` followed by `PING\r\n` returns `+PONG\r\n`
- [ ] `ECHO hello` returns `$5\r\nhello\r\n`
- [ ] An unknown command returns a RESP error line beginning with `-ERR`
- [ ] Multiple clients can connect simultaneously and each gets correct responses
- [ ] `go vet ./...` reports no issues
---
