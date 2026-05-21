package main

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
		expires: make(map[string]time.Time),
	}
}

// isExpiredLocked checks expiry. Must be called with at least RLock held
func (s *Store) isExpiredLocked(key string) bool {
	deadline, ok := s.expires[key]
	if !ok {
		return false
	}
	return time.Now().After(deadline)
}

// Set stores key -> value, with overwriting, uses write lock
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	delete(s.expires, key)
}

func (s *Store) SetWithTTL(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	s.expires[key] = time.Now().Add(ttl)
}

// Get return the value for a key if it exists, uses read lock
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isExpiredLocked(key) {
		return "", false
	}
	v, ok := s.data[key]
	return v, ok
}

// Del removes the key, return true if key existes, uses write lock
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
		delete(s.expires, key)
	}
	return ok
}

func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}

	// Treat a logically-expired key (not yet evicted) as nonexistent — Redis semantics.
	if s.isExpiredLocked(key) {
		return false
	}

	s.expires[key] = time.Now().Add(ttl)
	return true
}

// TTL returns the remaining time-to-live for a key.
// Returns -1 if the key exists but has no expiry.
// Returns -2 if the key does not exist or has already expired.
func (s *Store) TTL(key string) time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.data[key]; !ok {
		return -2 * time.Second
	}

	if s.isExpiredLocked(key) {
		return -2 * time.Second
	}

	deadline, ok := s.expires[key]
	if !ok {
		return -1 * time.Second
	}
	return time.Until(deadline)
}

// Persist removes the expiry from a key
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}

	_, had := s.expires[key]
	delete(s.expires, key)
	return had
}

// Len returns number of keys, uses read lock
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// String returns debugging information, uses read lock
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store(%d keys)", len(s.data))
}

// evictExpired is the active (proactive) side of expiry; Get is the lazy side.
// Together they ensure expired keys are neither returned nor held indefinitely.
func (s *Store) evictExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for key, deadline := range s.expires {
		if now.After(deadline) {
			delete(s.data, key)
			delete(s.expires, key)
		}
	}
}
