# Stage 10 — Pub/Sub: SUBSCRIBE, PUBLISH

In this final stage you implement the Redis Pub/Sub messaging model. Clients can subscribe to named channels and receive messages published by other clients. This requires a fundamentally different connection model: a subscribed connection stops being a request-response client and becomes a long-lived listener. You will use Go channels as in-process message queues and goroutines for fan-out.

---

## Implementation Status — NOT STARTED

No Pub/Sub code exists in the codebase. Stage 9 (RDB) must be completed first.

When implementing, note that the project now uses the `internal/` package structure:

- `PubSub` type → `internal/pubsub/pubsub.go` (package `pubsub`). This file already exists.
- `handleSubscribe` function → `internal/handler/handler.go` (package `handler`). Place it alongside the existing `Dispatch` function in that file. Only move it to a new `internal/handler/subscribe.go` if `handler.go` grows unwieldy.
- `Dispatch` function signature must be updated to accept `net.Conn` (so it can hand off to `handleSubscribe`) and `*pubsub.PubSub` (for PUBLISH and SUBSCRIBE).

---

## Sub-step A — Understand the Pub/Sub model

In Redis Pub/Sub:
- `SUBSCRIBE channel [channel ...]` — the client registers interest in one or more channels. The connection is now in subscribe mode: the only valid commands are `SUBSCRIBE`, `UNSUBSCRIBE`, and `PING`.
- `PUBLISH channel message` — any client (in normal mode) sends a message to a channel. All current subscribers receive it.
- Messages are not persisted. If no subscriber is listening when `PUBLISH` is called, the message is dropped.

The challenge in Go: a subscribed goroutine needs to block waiting for messages while still being interruptible (for `UNSUBSCRIBE` or client disconnect). This is a natural fit for `select` on Go channels.

---

## Sub-step B — Create pubsub.go

```go
package main

import (
	"sync"
)

// PubSub manages channel subscriptions.
type PubSub struct {
	mu   sync.RWMutex
	subs map[string]map[uint64]chan string // channel → {subscriber ID → delivery channel}
	next uint64                            // monotonically increasing subscriber ID
}

func NewPubSub() *PubSub {
	return &PubSub{
		subs: make(map[string]map[uint64]chan string),
	}
}

// Subscribe registers a subscriber for channel and returns:
//   - the delivery channel (read from this to receive messages)
//   - a unique subscriber ID (needed to unsubscribe)
func (ps *PubSub) Subscribe(channel string) (chan string, uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.subs[channel] == nil {
		ps.subs[channel] = make(map[uint64]chan string)
	}

	ps.next++
	id := ps.next
	// Buffer of 64 so a slow subscriber doesn't block PUBLISH.
	ch := make(chan string, 64)
	ps.subs[channel][id] = ch
	return ch, id
}

// Unsubscribe removes a subscriber from channel and closes its delivery channel.
func (ps *PubSub) Unsubscribe(channel string, id uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.subs[channel] == nil {
		return
	}
	if ch, ok := ps.subs[channel][id]; ok {
		close(ch)
		delete(ps.subs[channel], id)
	}
	if len(ps.subs[channel]) == 0 {
		delete(ps.subs, channel)
	}
}

// Publish sends message to all subscribers of channel.
// Returns the number of subscribers that received the message.
func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	subscribers := ps.subs[channel]
	if len(subscribers) == 0 {
		return 0
	}

	count := 0
	for _, ch := range subscribers {
		select {
		case ch <- message:
			count++
		default:
			// Subscriber's buffer is full; drop the message for this subscriber.
			// A production system would track dropped messages or block briefly.
		}
	}
	return count
}

// NumSubscribers returns the number of active subscribers for a channel.
func (ps *PubSub) NumSubscribers(channel string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.subs[channel])
}
```

The buffered delivery channel (`make(chan string, 64)`) decouples the publisher from the subscriber. `PUBLISH` completes in microseconds regardless of how many subscribers there are or how fast they consume messages. If a subscriber falls behind and the buffer fills, messages are dropped for that subscriber only — other subscribers are unaffected. This is the fan-out pattern.

---

## Sub-step C — Handle SUBSCRIBE in the connection loop

A subscribed connection cannot use the normal `dispatch` loop because `dispatch` returns a single response and reads the next command. A subscribed client instead blocks in a select loop.

Create a `handleSubscribe` function:

```go
package main

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// handleSubscribe enters subscribe mode for conn. channels is the initial list.
// r is the bufio.Reader already created by handleConn — pass it in rather than
// creating a new one, or buffered bytes already read from the conn will be lost.
func handleSubscribe(conn net.Conn, r *bufio.Reader, ps *PubSub, channels []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type subEntry struct {
		ch   chan string
		id   uint64
		name string
	}

	active := make(map[string]subEntry)

	// Subscribe to the initial channels.
	subscribeToChannels := func(names []string) {
		for _, name := range names {
			if _, already := active[name]; already {
				continue
			}
			ch, id := ps.Subscribe(name)
			active[name] = subEntry{ch: ch, id: id, name: name}
			// Send subscription confirmation.
			// Use a local variable name that does not shadow the resp package.
			confirmation := encodeSubscribeMessage("subscribe", name, len(active))
			conn.Write([]byte(confirmation))

			// Start a forwarder goroutine for this channel.
			go func(entry subEntry) {
				for {
					select {
					case payload, ok := <-entry.ch:
						if !ok {
							return // Channel was closed by Unsubscribe.
						}
						fwd := encodeMessageNotification(entry.name, payload)
						conn.Write([]byte(fwd))
					case <-ctx.Done():
						return
					}
				}
			}(active[name])
		}
	}

	subscribeToChannels(channels)

	// Read further commands from this connection (only SUBSCRIBE/UNSUBSCRIBE/PING).
	for {
		parts, err := readCommand(r)
		if err != nil {
			break // Client disconnected.
		}
		if len(parts) == 0 {
			continue
		}
		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "SUBSCRIBE":
			subscribeToChannels(parts[1:])
		case "UNSUBSCRIBE":
			for _, name := range parts[1:] {
				if entry, ok := active[name]; ok {
					ps.Unsubscribe(name, entry.id)
					delete(active, name)
					notification := encodeSubscribeMessage("unsubscribe", name, len(active))
					conn.Write([]byte(notification))
				}
			}
			if len(active) == 0 {
				return // All channels unsubscribed; exit subscribe mode.
			}
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		}
	}

	// Clean up all active subscriptions.
	cancel()
	for _, entry := range active {
		ps.Unsubscribe(entry.name, entry.id)
	}
}
```

**Note on the `bufio.Reader`:** There is no `newConnReader` helper. Pass the `*bufio.Reader` that `handleConn` already created (`r := bufio.NewReader(conn)`) directly into `handleSubscribe`. Creating a second `bufio.Reader` on the same `conn` would silently lose any bytes already buffered by the first reader — a subtle bug that only shows up when commands arrive back-to-back.

---

## Sub-step D — RESP encoding for Pub/Sub messages

Redis uses a three-element RESP array for subscribe/message notifications:

```go
// encodeSubscribeMessage encodes a subscribe/unsubscribe confirmation.
// Format: *3\r\n $<len>\r\n<kind>\r\n $<len>\r\n<channel>\r\n :<count>\r\n
func encodeSubscribeMessage(kind, channel string, count int) string {
	var sb strings.Builder
	sb.WriteString("*3\r\n")
	sb.WriteString(encodeBulkString(kind))
	sb.WriteString(encodeBulkString(channel))
	fmt.Fprintf(&sb, ":%d\r\n", count)
	return sb.String()
}

// encodeMessageNotification encodes a message delivery to a subscriber.
// Format: *3\r\n $7\r\nmessage\r\n $<len>\r\n<channel>\r\n $<len>\r\n<msg>\r\n
func encodeMessageNotification(channel, message string) string {
	var sb strings.Builder
	sb.WriteString("*3\r\n")
	sb.WriteString(encodeBulkString("message"))
	sb.WriteString(encodeBulkString(channel))
	sb.WriteString(encodeBulkString(message))
	return sb.String()
}
```

---

## Sub-step E — Add SUBSCRIBE and PUBLISH to the main dispatch

Update `dispatch` (and pass `*PubSub` alongside `*Store`):

```go
case "SUBSCRIBE":
	if len(args) == 0 {
		return encodeError("wrong number of arguments for 'subscribe' command")
	}
	// Hand off to subscribe mode — this function does not return until
	// the client unsubscribes or disconnects.
	handleSubscribe(conn, r, ps, args)
	return "" // Response already sent inside handleSubscribe.

case "PUBLISH":
	if len(args) != 2 {
		return encodeError("wrong number of arguments for 'publish' command")
	}
	n := ps.Publish(args[0], args[1])
	return encodeInteger(int64(n))
```

Because `SUBSCRIBE` transfers control to `handleSubscribe`, the `dispatch` function needs access to both `conn` and the existing `*bufio.Reader`. Update its signature:

```go
func dispatch(conn net.Conn, r *bufio.Reader, parts []string, store *Store, aof *AOF, saver *RDBSaver, ps *PubSub) string
```

---

## Sub-step F — Update main.go

```go
func main() {
	store := NewStore()
	ps := NewPubSub()

	// ... AOF, RDB setup as before ...

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go handleConn(conn, store, aof, saver, ps)
	}
}

func handleConn(conn net.Conn, store *Store, aof *AOF, saver *RDBSaver, ps *PubSub) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		parts, err := readCommand(r)
		if err != nil {
			return
		}
		if len(parts) == 0 {
			continue
		}
		response := dispatch(conn, r, parts, store, aof, saver, ps)
		if response != "" {
			conn.Write([]byte(response))
		}
		if isWriteCommand(parts[0]) {
			aof.Write(parts)
		}
	}
}
```

---

## Sub-step G — Test with two redis-cli sessions

Terminal A (subscriber):

```bash
redis-cli -p 6379 SUBSCRIBE news
# Reading messages... (press Ctrl-C to quit)
# 1) "subscribe"
# 2) "news"
# 3) (integer) 1
```

Terminal B (publisher):

```bash
redis-cli -p 6379 PUBLISH news "hello subscribers"
# (integer) 1
```

Terminal A receives:

```
1) "message"
2) "news"
3) "hello subscribers"
```

Multiple subscribers:

```bash
# Terminal C
redis-cli -p 6379 SUBSCRIBE news sports

# Terminal B
redis-cli -p 6379 PUBLISH news "breaking"
# (integer) 2  — two subscribers received it
```

---

## What you learned in this stage

| Concept | Where used |
|---|---|
| Go channels as message queues | `chan string` delivery channel per subscriber |
| Buffered channels for decoupling | `make(chan string, 64)` — publisher never blocks |
| Fan-out pattern | `Publish` sends to all subscriber channels |
| `context.WithCancel` | Signalling forwarder goroutines to stop on disconnect |
| `select` with channel and context | Forwarder goroutine — message vs stop signal |
| Goroutine per subscription | One forwarding goroutine per channel subscription |
| Subscribe mode connection state | `handleSubscribe` takes over the connection loop |
| RESP array for push notifications | `encodeSubscribeMessage`, `encodeMessageNotification` |
| `sync.RWMutex` on the sub map | `PubSub` — concurrent publish and subscribe |
| Closing a channel to signal goroutines | `Unsubscribe` closes the delivery channel |

---

## Checklist — You have completed mykvstore

- [ ] `SUBSCRIBE news` in terminal A enters subscribe mode correctly
- [ ] `PUBLISH news "hello"` in terminal B returns `(integer) 1`
- [ ] Terminal A receives the message formatted as a three-element array
- [ ] Two subscribers both receive the same published message
- [ ] `UNSUBSCRIBE` exits subscribe mode when all channels are removed
- [ ] Disconnecting a subscriber goroutine exits cleanly (no goroutine leak)
- [ ] `PUBLISH` to a channel with no subscribers returns `(integer) 0`
- [ ] Normal commands (`GET`, `SET`) still work from non-subscribed connections
- [ ] `go run -race .` shows no data races across Pub/Sub and Store operations
- [ ] `go build .` produces a clean binary with no errors or warnings
---
