package handler

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"mykvstore/internal/resp"
	"mykvstore/internal/store"
)

func Dispatch(parts []string, s *store.Store) string {
	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	switch cmd {
	case "PING":
		if len(args) == 0 {
			return resp.EncodeSimpleString("PONG")
		}
		return resp.EncodeBulkString(args[0])

	case "ECHO":
		if len(args) == 0 {
			return resp.EncodeError("wrong number of arguments for 'echo' command")
		}
		return resp.EncodeBulkString(args[0])

	case "SET":
		if len(args) < 2 {
			return resp.EncodeError("wrong number of arguments for 'set' command")
		}
		key, value := args[0], args[1]
		opts := args[2:]

		var ttl time.Duration
		for i := 0; i < len(opts)-1; i++ {
			switch strings.ToUpper(opts[i]) {
			case "EX":
				n, err := strconv.Atoi(opts[i+1])
				if err != nil || n <= 0 {
					return resp.EncodeError("invalid expire time in 'set' command")
				}
				ttl = time.Duration(n) * time.Second
				i++
			case "PX":
				n, err := strconv.Atoi(opts[i+1])
				if err != nil || n <= 0 {
					return resp.EncodeError("invalid expire time in 'set' command")
				}
				ttl = time.Duration(n) * time.Millisecond
				i++
			}
		}
		if ttl > 0 {
			s.SetWithTTL(key, value, ttl)
		} else {
			s.Set(key, value)
		}
		return resp.EncodeSimpleString("OK")

	case "GET":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'get' command")
		}
		val, ok := s.Get(args[0])
		if !ok {
			return resp.EncodeNull()
		}
		return resp.EncodeBulkString(val)

	case "DEL":
		if len(args) == 0 {
			return resp.EncodeError("wrong number of arguments for 'del' command")
		}
		var deleted int64
		for _, key := range args {
			if s.Del(key) {
				deleted++
			}
		}
		return resp.EncodeInteger(deleted)

	case "EXPIRE":
		if len(args) != 2 {
			return resp.EncodeError("wrong number of arguments for 'expire' command")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return resp.EncodeError("value is not an integer or out of range")
		}
		if s.Expire(args[0], time.Duration(n)*time.Second) {
			return resp.EncodeInteger(1)
		}
		return resp.EncodeInteger(0)

	case "TTL":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'ttl' command")
		}
		d := s.TTL(args[0])
		return resp.EncodeInteger(int64(d.Seconds()))

	case "PERSIST":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'persist' command")
		}
		if s.Persist(args[0]) {
			return resp.EncodeInteger(1)
		}
		return resp.EncodeInteger(0)

	case "EXISTS":
		if len(args) == 0 {
			return resp.EncodeError("wrong number of arguments for 'exists' command")
		}
		var count int64
		for _, key := range args {
			if s.Exists(key) {
				count++
			}
		}
		return resp.EncodeInteger(count)

	case "KEYS":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'keys' command")
		}
		return resp.EncodeArray(s.Keys(args[0]))

	case "INCR":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'incr' command")
		}
		n, err := s.Incr(args[0], 1)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "INCRBY":
		if len(args) != 2 {
			return resp.EncodeError("wrong number of arguments for 'incrby' command")
		}
		delta, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.EncodeError("value is not an integer or out of range")
		}
		n, err := s.Incr(args[0], delta)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "DECR":
		if len(args) != 1 {
			return resp.EncodeError("wrong number of arguments for 'decr' command")
		}
		n, err := s.Incr(args[0], -1)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "DECRBY":
		if len(args) != 2 {
			return resp.EncodeError("wrong number of arguments for 'decrby' command")
		}
		delta, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return resp.EncodeError("value is not an integer or out of range")
		}
		n, err := s.Incr(args[0], -delta)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "APPEND":
		if len(args) != 2 {
			return resp.EncodeError("wrong number of arguments for 'append' command")
		}
		n := s.Append(args[0], args[1])
		return resp.EncodeInteger(int64(n))

	case "COMMAND":
		return "*0\r\n"

	default:
		return resp.EncodeError(fmt.Sprintf("unknown command '%s'", parts[0]))
	}
}
