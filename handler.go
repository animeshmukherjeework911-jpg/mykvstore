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
		// PING with a message echose the message as a bulk string
		return encodeBulkString(args[0])

	case "ECHO":
		if len(args) == 0 {
			return encodeError("wrong number of arguments for 'echo' command")
		}
		return encodeBulkString(args[0])

	case "COMMAND":
		// redis-cli sends COMMAND DOCS on startup; return an empty array
		return "*0\r\n"
	default:
		return encodeError(fmt.Sprintf("unknonw command '%s'", parts[0]))
	}
}
