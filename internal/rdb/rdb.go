package rdb

import (
	"encoding/gob"
	"fmt"
	"log"
	"mykvstore/internal/store"
	"os"
	"sync"
	"time"
)

const DefaultRDBPath = "dump.rdb"

type Snapshot struct {
	Data    map[string]string
	Expires map[string]time.Time
	Lists   map[string][]string
	SavedAt time.Time
}

func SaveRDB(s *store.Store, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("rdb create tmp: %w", err)
	}

	data, expires, lists := s.TakeSnapshot()

	snap := Snapshot{
		Data:    data,
		Expires: expires,
		Lists:   lists,
		SavedAt: time.Now(),
	}

	enc := gob.NewEncoder(f)

	if err := enc.Encode(snap); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("rdb encode : %w", err)
	}
	f.Close()

	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rdb rename: %w", err)
	}
	return nil
}

func LoadRDB(s *store.Store, path string) (time.Time, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return time.Time{}, nil
	}

	if err != nil {
		return time.Time{}, fmt.Errorf("rdb open: %w", err)
	}

	defer f.Close()
	var snap Snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return time.Time{}, fmt.Errorf("rdb decode: %w", err)
	}

	s.RestoreSnapshot(snap.Data, snap.Expires, snap.Lists)
	log.Printf("RDB loaded: %d string keys, %d list keys (saved at %s)", len(snap.Data), len(snap.Lists), snap.SavedAt.Format(time.RFC3339))

	return snap.SavedAt, nil

}

type RDBSaver struct {
	store    *store.Store
	path     string
	saveCh   chan struct{}
	stopCh   chan struct{}
	mu       sync.Mutex
	lastSave time.Time
	wg       sync.WaitGroup
}

func NewRDBSaver(s *store.Store, path string) *RDBSaver {
	return &RDBSaver{
		store:  s,
		path:   path,
		saveCh: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
	}
}

func (r *RDBSaver) Start(interval time.Duration) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.save("periodic")
			case <-r.saveCh:
				r.save("BGSAVE")
			case <-r.stopCh:
				r.save("shutdown")
				return
			}
		}
	}()
}

// Trigger requests an immediate background save (non-blocking).
func (r *RDBSaver) Trigger() {
	select {
	case r.saveCh <- struct{}{}:
	default:
	}
}

// LastSave returns the time of the most recent succesful save.
func (r *RDBSaver) LastSave() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastSave
}

// Stop shuts down the saver gracefully, preforming one final save.
func (r *RDBSaver) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

func (r *RDBSaver) save(reason string) {
	start := time.Now()
	if err := SaveRDB(r.store, r.path); err != nil {
		log.Printf("RDB save (%s) failed: %v", reason, err)
		return
	}

	r.mu.Lock()
	r.lastSave = time.Now()
	r.mu.Unlock()
	log.Printf("RDB save (%s) completed in %v", reason, time.Since(start))

}
