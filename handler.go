package main

import (
	"fmt"
	"strings"
)

func dispatch(parts []string, store *Store) string {

	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	switch cmd {
	case "PING":
		if len(args) == 0 {
			return encodeSimpleString("PONG")
		}
		// PING with a message echose the message as a bulk string
		return encodeBulkString(args[0])

	case "ECHO":
		if len(args) == 0 {
			return encodeError("wrong number of arguments for 'echo' command")
		}
		return encodeBulkString(args[0])

	case "SET":
		if len(args) < 2 {
			return encodeError("wrong number of arguments for 'set' command")
		}
		store.Set(args[0], args[1])
		return encodeSimpleString("OK")

	case "GET":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'get' command")
		}
		val, ok := store.Get(args[0])
		if !ok {
			return encodeNull()
		}
		return encodeBulkString(val)

	case "DEL":
		if len(args) == 0 {
			return encodeError("wrong number of arguments for 'del' command")
		}
		var deleted int64
		for _, key := range args {
			if store.Del(key) {
				deleted++
			}
		}
		return encodeInteger(deleted)

	case "COMMAND":
		// redis-cli sends COMMAND DOCS on startup; return an empty array
		return "*0\r\n"
	default:
		return encodeError(fmt.Sprintf("unknonw command '%s'", parts[0]))
	}
}
