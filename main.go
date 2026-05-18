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
		line := scanner.Text()

		if line == "" {
			continue
		}

		parts := strings.Fields(line)
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
	case "PING" :
		if len(args) == 0 {
			return "+PONG\r\n"
		}
		// PING with a message echose the message as a bulk string
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
