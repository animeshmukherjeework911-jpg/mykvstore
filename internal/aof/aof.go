package aof

import (
	"bufio"
	"fmt"
	"log"
	"mykvstore/internal/resp"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const DefaultAOFPath = "aof/aof.log"

// AOF handles append-only file persistence
type AOF struct {
	mu   sync.Mutex
	file *os.File
	bw   *bufio.Writer
}

// OpenAOF opens (or creates) the AOF file for appending
func OpenAOF(path string) (*AOF, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create aof dir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open aof: %w", err)
	}

	return &AOF{
		file: f,
		bw:   bufio.NewWriterSize(f, 4096),
	}, nil
}

// Write appends a RESP-encoded command to the AOF.
func (a *AOF) Write(parts []string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Encode as RESP array
	fmt.Fprintf(a.bw, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(a.bw, "$%d\r\n%s\r\n", len(p), p)
	}

	// Flush to the OS buffer, for stronger durability you would also call
	// a.file.sync() here, but that has a significant preformance cost
	a.bw.Flush()
}

// Sync fluses and fsyncs the file to disk
// Call this if you need a durability guarantee stronger than OS buffering

func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.bw.Flush(); err != nil {
		return err
	}

	return a.file.Sync()
}

// Close flushes and closes the AOF file
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.bw.Flush()
	return a.file.Close()
}

// Replay reads the AOF from the beginning and calls apply for every command.
// The caller supplies apply — typically a closure over handler.Dispatch.
func (a *AOF) Replay(apply func([]string)) error {
	if _, err := a.file.Seek(0, 0); err != nil {
		return fmt.Errorf("aof seek: %w", err)
	}

	r := bufio.NewReader(a.file)
	replayed := 0

	for {
		parts, err := resp.ReadCommand(r)
		if err != nil {
			break
		}
		if len(parts) == 0 {
			continue
		}
		apply(parts)
		replayed++
	}

	log.Printf("AOF replay: %d commands", replayed)

	if _, err := a.file.Seek(0, 2); err != nil {
		return fmt.Errorf("aof seek to end: %w", err)
	}
	return nil
}

// ReplayAfter replays AOF commands recorded after cutoff.
// In this implementation the full AOF is replayed; the RDB already has
// the canonical state, so re-applying SET/DEL commands is idempotent.
func (a *AOF) ReplayAfter(apply func([]string), cutoff time.Time) error {
	return a.Replay(apply)
}