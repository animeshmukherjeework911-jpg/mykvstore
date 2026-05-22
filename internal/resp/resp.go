package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ReadCommand reads one command from r, supporting both inline (space-separated)
// and RESP array (*) formats. Returns nil, nil on a blank line.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == '*' {
		return readArray(r)
	}

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

// readArray parses a RESP array from r. The leading '*' has already been consumed.
func readArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %d", err)
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

// readBulkString parses a single RESP bulk string ($<len>\r\n<data>\r\n) from r.
// Returns an empty string for null bulk strings (length < 0).
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
		return "", nil
	}

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf[:length]), nil
}

// EncodeSimpleString encodes s as a RESP simple string (+<s>\r\n).
func EncodeSimpleString(s string) string {
	return "+" + s + "\r\n"
}

// EncodeError encodes msg as a RESP error (-ERR <msg>\r\n).
func EncodeError(msg string) string {
	return "-ERR" + msg + "\r\n"
}

// EncodeInteger encodes n as a RESP integer (:<n>\r\n).
func EncodeInteger(n int64) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// EncodeBulkString encodes s as a RESP bulk string ($<len>\r\n<s>\r\n).
func EncodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// EncodeNull encodes a RESP null bulk string ($-1\r\n).
func EncodeNull() string {
	return "$-1\r\n"
}

// EncodeArray encodes items as a RESP array (*<count>\r\n followed by bulk strings).
func EncodeArray(items []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(items))
	for _, item := range items {
		sb.WriteString(EncodeBulkString(item))
	}
	return sb.String()
}
