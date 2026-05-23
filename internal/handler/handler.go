package handler

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"mykvstore/internal/resp"
	"mykvstore/internal/store"
)

func wrongArgs(cmd string) string {
	return resp.EncodeError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd))
}

func boolReply(b bool) string {
	if b {
		return resp.EncodeInteger(1)
	}
	return resp.EncodeInteger(0)
}

func incrBy(args []string, cmd string, s *store.Store) string {
	if len(args) != 2 {
		return wrongArgs(cmd)
	}
	delta, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return resp.EncodeError("value is not an integer or out of range")
	}
	if cmd == "decrby" {
		delta = -delta
	}
	n, err := s.Incr(args[0], delta)
	if err != nil {
		return resp.EncodeError(err.Error())
	}
	return resp.EncodeInteger(n)
}

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
			return wrongArgs("echo")
		}
		return resp.EncodeBulkString(args[0])

	case "SET":
		if len(args) < 2 {
			return wrongArgs("set")
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
			return wrongArgs("get")
		}
		val, ok := s.Get(args[0])
		if !ok {
			return resp.EncodeNull()
		}
		return resp.EncodeBulkString(val)

	case "DEL":
		if len(args) == 0 {
			return wrongArgs("del")
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
			return wrongArgs("expire")
		}
		n, err := strconv.Atoi(args[1])
		if err != nil {
			return resp.EncodeError("value is not an integer or out of range")
		}
		return boolReply(s.Expire(args[0], time.Duration(n)*time.Second))

	case "TTL":
		if len(args) != 1 {
			return wrongArgs("ttl")
		}
		return resp.EncodeInteger(int64(s.TTL(args[0]).Seconds()))

	case "PERSIST":
		if len(args) != 1 {
			return wrongArgs("persist")
		}
		return boolReply(s.Persist(args[0]))

	case "EXISTS":
		if len(args) == 0 {
			return wrongArgs("exists")
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
			return wrongArgs("keys")
		}
		return resp.EncodeArray(s.Keys(args[0]))

	case "INCR":
		if len(args) != 1 {
			return wrongArgs("incr")
		}
		n, err := s.Incr(args[0], 1)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "DECR":
		if len(args) != 1 {
			return wrongArgs("decr")
		}
		n, err := s.Incr(args[0], -1)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeInteger(n)

	case "INCRBY":
		return incrBy(args, "incrby", s)

	case "DECRBY":
		return incrBy(args, "decrby", s)

	case "APPEND":
		if len(args) != 2 {
			return wrongArgs("append")
		}
		return resp.EncodeInteger(int64(s.Append(args[0], args[1])))

	case "LPUSH":
		if len(args) < 2 {
			return wrongArgs("lpush")
		}
		n, err := s.LPush(args[0], args[1:]...)

		if err != nil {
			return "-" + err.Error() + "\r\n"
		}
		return resp.EncodeInteger(int64(n))

	case "RPUSH":
		if len(args) < 2 {
			return wrongArgs("rpush")
		}
		n, err := s.RPush(args[0], args[1:]...)

		if err != nil {
			return "-" + err.Error() + "\r\n"
		}
		return resp.EncodeInteger(int64(n))

	case "LPOP":
		if len(args) != 1 {
			return wrongArgs("lpop")
		}
		val, ok, err := s.LPop(args[0])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		if !ok {
			return resp.EncodeNull()
		}
		return resp.EncodeBulkString(val)

	case "RPOP":
		if len(args) != 1 {
			return wrongArgs("rpop")
		}
		val, ok, err := s.RPop(args[0])
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		if !ok {
			return resp.EncodeNull()
		}
		return resp.EncodeBulkString(val)

	case "LLEN":
		if len(args) != 1 {
			return wrongArgs("llen")
		}

		n, err := s.LLen(args[0])
		if err != nil {
			return "-" + err.Error() + "\r\n"
		}
		return resp.EncodeInteger(int64(n))

	case "LRANGE":
		if len(args) != 3 {
			return wrongArgs("lrange")
		}
		start, err1 := strconv.Atoi(args[1])
		stop, err2 := strconv.Atoi(args[2])

		if err1 != nil || err2 != nil {
			return resp.EncodeError("value is not an integer or out of range")
		}

		items, err := s.LRange(args[0], start, stop)
		if err != nil {
			return "-" + err.Error() + "\r\n"
		}
		return resp.EncodeArray(items)

	case "COMMAND":
		return "*0\r\n"

	default:
		return resp.EncodeError(fmt.Sprintf("unknown command '%s'", parts[0]))
	}
}
