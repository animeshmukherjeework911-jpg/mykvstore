package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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
		key, value := args[0], args[1]
		opts := args[2:]

		var ttl time.Duration

		for i := 0; i < len(opts)-1; i++ {
			switch strings.ToUpper(opts[i]) {
			case "EX":
				n, err := strconv.Atoi(opts[i+1])
				if err != nil || n <= 0 {
					return encodeError("invalid expire time in 'set' command")
				}
				ttl = time.Duration(n) * time.Second
				i++ // skip the consumed value token
			case "PX":
				n, err := strconv.Atoi(opts[i+1])
				if err != nil || n <= 0 {
					return encodeError("invalid expire time in 'set' command")
				}
				ttl = time.Duration(n) * time.Millisecond
				i++ // skip the consumed value token
			}
		}
		if ttl > 0 {
			store.SetWithTTL(key, value, ttl)
		} else {
			store.Set(args[0], args[1])
		}

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

	case "EXPIRE":
		if len(args) != 2 {
			return encodeError("wrong number of arguments for 'expire' command")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return encodeError("value is not an integer or out of range")
		}
		if store.Expire(args[0], time.Duration(n)*time.Second) {
			return encodeInteger(1)
		}
		return encodeInteger(0)

	case "TTL":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'ttl' command")
		}
		d := store.TTL(args[0])
		// Truncation to integer seconds is intentional (Redis TTL contract).
		// Sentinel durations (-1s, -2s) convert exactly to -1, -2.
		return encodeInteger(int64(d.Seconds()))

	case "PERSIST":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'persist' command")
		}
		if store.Persist(args[0]) {
			return encodeInteger(1)
		}
		return encodeInteger(0)

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

	case "KEYS":
		if len(args) != 1 {
			return encodeError("wrong number of arguments for 'keys' command")
		}
		keys := store.Keys(args[0])
		return encodeArray(keys)

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
			return encodeError("wrong number of arguments for 'incr' command")
		}
		n, err := store.Incr(args[0], -1)
		if err != nil {
			return encodeError(err.Error())
		}
		return encodeInteger(n)

	case "DECRBY":
		if len(args) != 2 {
			return encodeError("wrong number of arguments for 'incrby' command")
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
	case "APPEND":
		if len(args) != 2 {
			return encodeError("wrong number of arguments for 'append' command")
		}
		n := store.Append(args[0], args[1])
		return encodeInteger(int64(n))
	case "COMMAND":
		// redis-cli sends COMMAND DOCS on startup; return an empty array
		return "*0\r\n"
	default:
		return encodeError(fmt.Sprintf("unknonw command '%s'", parts[0]))
	}
}
