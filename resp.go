package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// readCommand reads one command from r, supporting both inline (space-separated)
// and RESP array (*) formats. Returns nil, nil on a blank line.
func readCommand(r *bufio.Reader) ([]string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Peek at the first byte to decide which format we have.
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
	// we have consumed * read the count

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
		s, err := readBulkStrings(r)
		if err != nil {
			return nil, err
		}

		parts = append(parts, s)
	}
	return parts, nil

}

// readBulkStrings parses a single RESP bulk string ($<len>\r\n<data>\r\n) from r.
// Returns an empty string for null bulk strings (length < 0).
func readBulkStrings(r *bufio.Reader) (string, error) {
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

// encodeSimpleString encodes s as a RESP simple string (+<s>\r\n).
func encodeSimpleString(s string) string {
	return "+" + s + "\r\n"
}

// encodeError encodes msg as a RESP error (-ERR <msg>\r\n).
func encodeError(msg string) string {
	return "-ERR" + msg + "\r\n"
}

// encodeInteger encodes n as a RESP integer (:<n>\r\n).
func encodeInteger(n int64) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// encodeBulkString encodes s as a RESP bulk string ($<len>\r\n<s>\r\n).
func encodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// encodeNull encodes a RESP null bulk string ($-1\r\n).
func encodeNull() string {
	return "$-1\r\n"
}

// encodeArray encodes items as a RESP array (*<count>\r\n followed by bulk strings).
func encodeArray(items []string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(items))
	for _, item := range items {
		sb.WriteString(encodeBulkString(item))
	}
	return sb.String()
}
