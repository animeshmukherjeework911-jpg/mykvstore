package store

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	mu      sync.RWMutex
	data    map[string]string
	expires map[string]time.Time
	lists   map[string][]string
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]string),
		expires: make(map[string]time.Time),
		lists:   make(map[string][]string),
	}
}

const wrongTypeErr = "WRONGTYPE Operation against a key holding the wrong kind of value"

// LPush prepends one or more values to the list at key.
// Returns the new length of the list after the push
// Returns an error string if the key holds a non-List value
func (s *Store) LPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}

	for _, v := range values {
		s.lists[key] = append([]string{v}, s.lists[key]...)
	}

	return len(s.lists[key]), nil
}

// RPush appends one or more values to the list at key
// Returns the new length of the list.

func (s *Store) RPush(key string, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}

	s.lists[key] = append(s.lists[key], values...)
	return len(s.lists[key]), nil
}

// LPop removes and returns the first element of the list
// Returns ("", false) if the list is empty or does not exist

func (s *Store) LPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, isString := s.data[key]; isString {
		return "", false, fmt.Errorf(wrongTypeErr)
	}
	list, ok := s.lists[key]

	if !ok || len(list) == 0 {
		return "", false, nil
	}

	val := list[0]
	s.lists[key] = list[1:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return val, true, nil
}

// RPop removes and returns the last element of the list
func (s *Store) RPop(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, isString := s.data[key]; isString {
		return "", false, fmt.Errorf(wrongTypeErr)
	}

	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return "", false, nil
	}

	n := len(list)
	val := list[n-1]
	s.lists[key] = list[:n-1]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}
	return val, true, nil
}

// LLen returns the length of the list at key.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, isString := s.data[key]; isString {
		return 0, fmt.Errorf(wrongTypeErr)
	}
	return len(s.lists[key]), nil
}

// LRange returns the elements of the list between start and stop (inclusive)
// Negative indices count from the end; -1 is the last element.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, isString := s.data[key]; isString {
		return nil, fmt.Errorf(wrongTypeErr)
	}
	list := s.lists[key]
	n := len(list)
	if n == 0 {
		return []string{}, nil
	}

	// Resolved negative indices
	if start < 0 {
		start = n + start
	}
	if stop < 0 {
		stop = n + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}

	if start > stop {
		return []string{}, nil
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result, nil
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
	_, inData := s.data[key]
	_, inLists := s.lists[key]
	if inData {
		delete(s.data, key)
		delete(s.expires, key)
	}
	if inLists {
		delete(s.lists, key)
	}
	return inData || inLists
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

// EvictExpired is the active (proactive) side of expiry; Get is the lazy side.
// Together they ensure expired keys are neither returned nor held indefinitely.
func (s *Store) EvictExpired() {
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

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.isExpiredLocked(key) {
		return false
	}
	_, inData := s.data[key]
	_, inLists := s.lists[key]
	return inData || inLists
}

func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []string
	for key := range s.data {
		if s.isExpiredLocked(key) {
			continue
		}
		if matchGlob(pattern, key) {
			result = append(result, key)
		}
	}
	for key := range s.lists {
		if matchGlob(pattern, key) {
			result = append(result, key)
		}
	}
	return result
}

func matchGlob(pattern, key string) bool {
	if pattern == "*" {
		return true
	}

	if !strings.Contains(pattern, "*") {
		return pattern == key
	}

	parts := strings.Split(pattern, "*")
	pos := 0

	for i, part := range parts {
		if part == "" {
			continue
		}

		idx := strings.Index(key[pos:], part)
		if idx == -1 {
			return false
		}

		// The first segment must be a prefix match
		if i == 0 && idx != 0 {
			return false
		}

		pos += idx + len(part)
	}

	if !strings.HasSuffix(pattern, "*") {
		last := parts[len(parts)-1]

		if last != "" && !strings.HasSuffix(key, last) {
			return false
		}
	}

	return true
}

func (s *Store) Incr(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var current int64
	if val, ok := s.data[key]; ok {
		var err error
		current, err = strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer or out of range")
		}
	}
	next := current + delta
	s.data[key] = strconv.FormatInt(next, 10)
	return next, nil
}

func (s *Store) Append(key, suffix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = s.data[key] + suffix
	return len(s.data[key])
}

func (s *Store) TakeSnapshot() (map[string]string,
	map[string]time.Time, map[string][]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string]string, len(s.data))
	for k, v := range s.data {
		data[k] = v
	}

	expires := make(map[string]time.Time, len(s.expires))
	for k, v := range s.expires {
		expires[k] = v
	}

	lists := make(map[string][]string, len(s.lists))
	for k, v := range s.lists {
		cp := make([]string, len(v))
		copy(cp, v)
		lists[k] = cp
	}

	return data, expires, lists
}

func (s *Store) RestoreSnapshot(data map[string]string, expires map[string]time.Time, lists map[string][]string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range data {
		s.data[k] = v 
	}

	for k, v := range expires {
		s.expires[k] = v
	}

	for k, v := range lists {
		s.lists[k] = v
	}
}
