package store_test

import (
	"testing"
	"time"

	"mykvstore/internal/store"
)

func newStore() *store.Store { return store.NewStore() }

// Stage 4 — SET / GET / DEL

func TestSetGet(t *testing.T) {
	s := newStore()
	s.Set("foo", "bar")
	val, ok := s.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("expected bar got %q ok=%v", val, ok)
	}
}

func TestGetMissing(t *testing.T) {
	s := newStore()
	_, ok := s.Get("nosuchkey")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestSetOverwrite(t *testing.T) {
	s := newStore()
	s.Set("k", "v1")
	s.Set("k", "v2")
	val, _ := s.Get("k")
	if val != "v2" {
		t.Fatalf("expected v2 got %q", val)
	}
}

func TestDel(t *testing.T) {
	s := newStore()
	s.Set("k", "v")
	if !s.Del("k") {
		t.Fatal("Del should return true for existing key")
	}
	if s.Del("k") {
		t.Fatal("Del should return false for already-deleted key")
	}
	_, ok := s.Get("k")
	if ok {
		t.Fatal("Get after Del should miss")
	}
}

func TestDelMultipleKeys(t *testing.T) {
	s := newStore()
	s.Set("k1", "a")
	s.Set("k2", "b")
	deleted := 0
	for _, key := range []string{"k1", "k2", "k3"} {
		if s.Del(key) {
			deleted++
		}
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}
}

// Stage 5 — TTL / EXPIRE / PERSIST

func TestSetWithTTLExpires(t *testing.T) {
	s := newStore()
	s.SetWithTTL("k", "v", 50*time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key should exist immediately after SetWithTTL")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should have expired")
	}
}

func TestTTLPositive(t *testing.T) {
	s := newStore()
	s.SetWithTTL("k", "v", 10*time.Second)
	d := s.TTL("k")
	if d <= 0 {
		t.Fatalf("expected positive TTL, got %v", d)
	}
}

func TestTTLNoExpiry(t *testing.T) {
	s := newStore()
	s.Set("k", "v")
	d := s.TTL("k")
	if d != -1*time.Second {
		t.Fatalf("expected -1s for key with no expiry, got %v", d)
	}
}

func TestTTLMissing(t *testing.T) {
	s := newStore()
	d := s.TTL("nosuchkey")
	if d != -2*time.Second {
		t.Fatalf("expected -2s for missing key, got %v", d)
	}
}

func TestExpireExistingKey(t *testing.T) {
	s := newStore()
	s.Set("k", "v")
	if !s.Expire("k", 50*time.Millisecond) {
		t.Fatal("Expire should return true for existing key")
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key should have expired after Expire")
	}
}

func TestExpireNonexistentKey(t *testing.T) {
	s := newStore()
	if s.Expire("nosuchkey", time.Second) {
		t.Fatal("Expire on missing key should return false")
	}
}

func TestPersistRemovesExpiry(t *testing.T) {
	s := newStore()
	s.SetWithTTL("k", "v", 100*time.Millisecond)
	if !s.Persist("k") {
		t.Fatal("Persist should return true when expiry existed")
	}
	d := s.TTL("k")
	if d != -1*time.Second {
		t.Fatalf("expected -1s after Persist, got %v", d)
	}
	time.Sleep(150 * time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key should survive after Persist removes expiry")
	}
}

func TestPersistNoExpiry(t *testing.T) {
	s := newStore()
	s.Set("k", "v")
	if s.Persist("k") {
		t.Fatal("Persist should return false when no expiry was set")
	}
}

// Stage 6 — EXISTS / KEYS / INCR / APPEND

func TestExistsPresent(t *testing.T) {
	s := newStore()
	s.Set("k", "v")
	if !s.Exists("k") {
		t.Fatal("Exists should return true for present key")
	}
}

func TestExistsMissing(t *testing.T) {
	s := newStore()
	if s.Exists("nosuchkey") {
		t.Fatal("Exists should return false for missing key")
	}
}

func TestExistsExpired(t *testing.T) {
	s := newStore()
	s.SetWithTTL("k", "v", 30*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	if s.Exists("k") {
		t.Fatal("Exists should return false for expired key")
	}
}

func TestKeysStar(t *testing.T) {
	s := newStore()
	s.Set("user:1", "a")
	s.Set("user:2", "b")
	s.Set("session:1", "c")
	keys := s.Keys("*")
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}
}

func TestKeysPrefix(t *testing.T) {
	s := newStore()
	s.Set("user:1", "a")
	s.Set("user:2", "b")
	s.Set("session:1", "c")
	keys := s.Keys("user:*")
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d: %v", len(keys), keys)
	}
}

func TestKeysExact(t *testing.T) {
	s := newStore()
	s.Set("foo", "bar")
	s.Set("foobar", "baz")
	keys := s.Keys("foo")
	if len(keys) != 1 || keys[0] != "foo" {
		t.Fatalf("expected [foo], got %v", keys)
	}
}

func TestIncrMissingKeyStartsAtZero(t *testing.T) {
	s := newStore()
	n, err := s.Incr("counter", 1)
	if err != nil || n != 1 {
		t.Fatalf("expected 1, got %d err=%v", n, err)
	}
}

func TestIncrExistingKey(t *testing.T) {
	s := newStore()
	s.Set("counter", "10")
	n, err := s.Incr("counter", 1)
	if err != nil || n != 11 {
		t.Fatalf("expected 11, got %d err=%v", n, err)
	}
}

func TestIncrByDelta(t *testing.T) {
	s := newStore()
	s.Set("counter", "10")
	n, _ := s.Incr("counter", 5)
	if n != 15 {
		t.Fatalf("expected 15, got %d", n)
	}
}

func TestDecrByNegativeDelta(t *testing.T) {
	s := newStore()
	s.Set("counter", "10")
	n, _ := s.Incr("counter", -3)
	if n != 7 {
		t.Fatalf("expected 7, got %d", n)
	}
}

func TestIncrNonInteger(t *testing.T) {
	s := newStore()
	s.Set("k", "notanint")
	_, err := s.Incr("k", 1)
	if err == nil {
		t.Fatal("expected error for non-integer value")
	}
}

func TestAppend(t *testing.T) {
	s := newStore()
	s.Set("greeting", "Hello")
	n := s.Append("greeting", ", World")
	if n != 12 {
		t.Fatalf("expected length 12, got %d", n)
	}
	val, _ := s.Get("greeting")
	if val != "Hello, World" {
		t.Fatalf("expected 'Hello, World', got %q", val)
	}
}

func TestAppendMissingKey(t *testing.T) {
	s := newStore()
	n := s.Append("k", "hello")
	if n != 5 {
		t.Fatalf("expected 5, got %d", n)
	}
}

// Stage 7 — LPUSH / RPUSH / LPOP / RPOP / LLEN / LRANGE

func TestRPushReturnsLength(t *testing.T) {
	s := newStore()
	n, err := s.RPush("k", "a", "b", "c")
	if err != nil || n != 3 {
		t.Fatalf("expected 3, got %d err=%v", n, err)
	}
}

func TestLPushPrependsInOrder(t *testing.T) {
	s := newStore()
	s.RPush("k", "a", "b", "c")
	s.LPush("k", "z")
	items, _ := s.LRange("k", 0, -1)
	if items[0] != "z" || len(items) != 4 {
		t.Fatalf("expected [z a b c], got %v", items)
	}
}

func TestLPopRemovesFirst(t *testing.T) {
	s := newStore()
	s.RPush("k", "a", "b", "c")
	val, ok, err := s.LPop("k")
	if err != nil || !ok || val != "a" {
		t.Fatalf("expected a, got %q ok=%v err=%v", val, ok, err)
	}
}

func TestRPopRemovesLast(t *testing.T) {
	s := newStore()
	s.RPush("k", "a", "b", "c")
	val, ok, err := s.RPop("k")
	if err != nil || !ok || val != "c" {
		t.Fatalf("expected c, got %q ok=%v err=%v", val, ok, err)
	}
}

func TestLPopEmptyList(t *testing.T) {
	s := newStore()
	_, ok, err := s.LPop("nosuchkey")
	if err != nil || ok {
		t.Fatalf("expected (false, nil) for missing key, got ok=%v err=%v", ok, err)
	}
}

func TestLRangeNegativeIndex(t *testing.T) {
	s := newStore()
	s.RPush("k", "a", "b", "c")
	items, err := s.LRange("k", 0, -1)
	if err != nil || len(items) != 3 || items[2] != "c" {
		t.Fatalf("LRANGE 0 -1: expected [a b c], got %v err=%v", items, err)
	}
}

func TestLLen(t *testing.T) {
	s := newStore()
	s.RPush("k", "a", "b", "c")
	s.LPop("k")
	n, err := s.LLen("k")
	if err != nil || n != 2 {
		t.Fatalf("expected 2, got %d err=%v", n, err)
	}
}

func TestLPopDeletesKeyWhenEmpty(t *testing.T) {
	s := newStore()
	s.RPush("k", "a")
	s.LPop("k")
	n, _ := s.LLen("k")
	if n != 0 {
		t.Fatalf("expected empty list after popping last element, got len %d", n)
	}
}

func TestWrongTypeError(t *testing.T) {
	s := newStore()
	s.Set("strkey", "hello")
	_, err := s.LPush("strkey", "x")
	if err == nil {
		t.Fatal("expected WRONGTYPE error for LPush on string key")
	}
}

// Concurrent access — run with go test -race

func TestConcurrentSetGet(t *testing.T) {
	s := newStore()
	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func(i int) {
			s.Set("key", "val")
			s.Get("key")
			if i == 49 {
				close(done)
			}
		}(i)
	}
	<-done
}
