// Black-box integration tests for mykvstore.
//
// TestMain builds the server binary, starts it on port 6399 (to avoid
// colliding with a real Redis on 6379), runs all tests, then shuts it down.
//
// Run from the test/blackbox directory:
//   go test -v ./...
package blackbox_test

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const serverAddr = "localhost:6399"
const serverBin = "/tmp/mykvstore_test_bin"

var ctx = context.Background()

func TestMain(m *testing.M) {
	// Resolve the project root (two levels up from test/blackbox/).
	wd, err := os.Getwd()
	if err != nil {
		panic("getwd: " + err.Error())
	}
	serverSrc := filepath.Dir(filepath.Dir(wd))

	// Build the server binary from the project root.
	build := exec.Command("go", "build", "-o", serverBin, ".")
	build.Dir = serverSrc
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		panic("failed to build server: " + err.Error())
	}

	// Start the server on a non-default port so it doesn't clash with real Redis.
	srv := exec.Command(serverBin, "-port", "6399")
	srv.Stdout = os.Stderr
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		panic("failed to start server: " + err.Error())
	}

	// Wait until the port is accepting connections (up to 3 seconds).
	ready := false
	for range 30 {
		conn, err := net.DialTimeout("tcp", serverAddr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		srv.Process.Kill()
		panic("server did not become ready in time")
	}

	code := m.Run()

	srv.Process.Kill()
	srv.Wait()
	os.Remove(serverBin)

	os.Exit(code)
}

// newClient returns a go-redis client pointed at the test server.
// Each test should call FlushAll (via DEL *) to start with a clean slate.
func newClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: serverAddr})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// flush deletes all keys visible via KEYS *.
func flush(t *testing.T, rdb *redis.Client) {
	t.Helper()
	keys, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("flush KEYS: %v", err)
	}
	if len(keys) > 0 {
		if err := rdb.Del(ctx, keys...).Err(); err != nil {
			t.Fatalf("flush DEL: %v", err)
		}
	}
}

// Stage 3 — PING / ECHO

func TestPing_Stage3(t *testing.T) {
	rdb := newClient(t)
	got, err := rdb.Ping(ctx).Result()
	if err != nil || got != "PONG" {
		t.Fatalf("PING: want PONG, got %q err=%v", got, err)
	}
}

func TestPingWithMessage_Stage3(t *testing.T) {
	rdb := newClient(t)
	got, err := rdb.Do(ctx, "PING", "hello").Text()
	if err != nil || got != "hello" {
		t.Fatalf("PING hello: want hello, got %q err=%v", got, err)
	}
}

func TestEcho_Stage3(t *testing.T) {
	rdb := newClient(t)
	got, err := rdb.Echo(ctx, "hello world").Result()
	if err != nil || got != "hello world" {
		t.Fatalf("ECHO: want 'hello world', got %q err=%v", got, err)
	}
}

func TestUnknownCommand_Stage3(t *testing.T) {
	rdb := newClient(t)
	err := rdb.Do(ctx, "NOSUCHCMD").Err()
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

// Stage 4 — SET / GET / DEL

func TestSetGet_Stage4(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	if err := rdb.Set(ctx, "foo", "bar", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	got, err := rdb.Get(ctx, "foo").Result()
	if err != nil || got != "bar" {
		t.Fatalf("GET foo: want bar, got %q err=%v", got, err)
	}
}

func TestGetMissing_Stage4(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	err := rdb.Get(ctx, "nosuchkey").Err()
	if err != redis.Nil {
		t.Fatalf("GET missing: want redis.Nil, got %v", err)
	}
}

func TestDelExisting_Stage4(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "foo", "bar", 0)
	n, err := rdb.Del(ctx, "foo").Result()
	if err != nil || n != 1 {
		t.Fatalf("DEL existing: want 1, got %d err=%v", n, err)
	}
}

func TestDelAlreadyGone_Stage4(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "foo", "bar", 0)
	rdb.Del(ctx, "foo")
	n, err := rdb.Del(ctx, "foo").Result()
	if err != nil || n != 0 {
		t.Fatalf("DEL already gone: want 0, got %d err=%v", n, err)
	}
}

func TestDelMultiKey_Stage4(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "k1", "a", 0)
	rdb.Set(ctx, "k2", "b", 0)
	// k3 does not exist
	n, err := rdb.Del(ctx, "k1", "k2", "k3").Result()
	if err != nil || n != 2 {
		t.Fatalf("DEL multi: want 2, got %d err=%v", n, err)
	}
}

// Stage 5 — TTL / EXPIRE / PERSIST

func TestSetWithEX_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "foo", "bar", 2*time.Second)
	got, err := rdb.Get(ctx, "foo").Result()
	if err != nil || got != "bar" {
		t.Fatalf("GET before expiry: want bar, got %q err=%v", got, err)
	}
}

func TestTTLPositive_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "foo", "bar", 10*time.Second)
	ttl, err := rdb.TTL(ctx, "foo").Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("TTL: want positive, got %v err=%v", ttl, err)
	}
}

func TestTTLNoExpiry_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "foo", "bar", 0)
	ttl, err := rdb.TTL(ctx, "foo").Result()
	// go-redis v9 returns time.Duration(-1) for the -1 sentinel (no expiry set).
	if err != nil || ttl != time.Duration(-1) {
		t.Fatalf("TTL no-expiry: want -1 (sentinel), got %v err=%v", ttl, err)
	}
}

func TestTTLMissingKey_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	ttl, err := rdb.TTL(ctx, "nosuchkey").Result()
	// go-redis v9 returns time.Duration(-2) for the -2 sentinel (key missing).
	if err != nil || ttl != time.Duration(-2) {
		t.Fatalf("TTL missing: want -2 (sentinel), got %v err=%v", ttl, err)
	}
}

func TestExpireAndWait_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	// go-redis Expire() truncates sub-second durations to 1s.
	// Use Set with a PX duration instead: go-redis sends SET key val PX N
	// for sub-second values, which our server supports.
	rdb.Set(ctx, "foo", "bar", 150*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	if err := rdb.Get(ctx, "foo").Err(); err != redis.Nil {
		t.Fatalf("GET after PX expiry: want Nil, got %v", err)
	}
}

func TestExpireNonexistent_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	n, err := rdb.Expire(ctx, "nosuchkey", 10*time.Second).Result()
	if err != nil || n {
		t.Fatalf("EXPIRE missing key: want false, got %v err=%v", n, err)
	}
}

func TestPersist_Stage5(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "counter", "42", 30*time.Second)
	n, err := rdb.Persist(ctx, "counter").Result()
	if err != nil || !n {
		t.Fatalf("PERSIST: want true, got %v err=%v", n, err)
	}
	ttl, _ := rdb.TTL(ctx, "counter").Result()
	// go-redis v9 returns time.Duration(-1) for the no-expiry sentinel.
	if ttl != time.Duration(-1) {
		t.Fatalf("TTL after PERSIST: want -1 (sentinel), got %v", ttl)
	}
}

// Stage 6 — EXISTS / KEYS / INCR / APPEND

func TestExistsPresent_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "a", "1", 0)
	n, err := rdb.Exists(ctx, "a").Result()
	if err != nil || n != 1 {
		t.Fatalf("EXISTS present: want 1, got %d err=%v", n, err)
	}
}

func TestExistsMissing_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	n, err := rdb.Exists(ctx, "b").Result()
	if err != nil || n != 0 {
		t.Fatalf("EXISTS missing: want 0, got %d err=%v", n, err)
	}
}

func TestExistsDuplicateKey_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "a", "1", 0)
	// EXISTS a a counts the key twice
	n, err := rdb.Exists(ctx, "a", "a").Result()
	if err != nil || n != 2 {
		t.Fatalf("EXISTS duplicate: want 2, got %d err=%v", n, err)
	}
}

func TestKeysPattern_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "user:1", "alice", 0)
	rdb.Set(ctx, "user:2", "bob", 0)
	rdb.Set(ctx, "session:1", "xyz", 0)

	keys, err := rdb.Keys(ctx, "user:*").Result()
	if err != nil || len(keys) != 2 {
		t.Fatalf("KEYS user:*: want 2 keys, got %v err=%v", keys, err)
	}
}

func TestKeysStar_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "a", "1", 0)
	rdb.Set(ctx, "b", "2", 0)

	keys, err := rdb.Keys(ctx, "*").Result()
	if err != nil || len(keys) != 2 {
		t.Fatalf("KEYS *: want 2 keys, got %v err=%v", keys, err)
	}
}

func TestIncrMissingKey_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	n, err := rdb.Incr(ctx, "counter").Result()
	if err != nil || n != 1 {
		t.Fatalf("INCR missing: want 1, got %d err=%v", n, err)
	}
}

func TestIncrExisting_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "counter", "10", 0)
	n, err := rdb.Incr(ctx, "counter").Result()
	if err != nil || n != 11 {
		t.Fatalf("INCR existing: want 11, got %d err=%v", n, err)
	}
}

func TestIncrBy_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "counter", "10", 0)
	n, err := rdb.IncrBy(ctx, "counter", 5).Result()
	if err != nil || n != 15 {
		t.Fatalf("INCRBY: want 15, got %d err=%v", n, err)
	}
}

func TestDecr_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "counter", "10", 0)
	n, err := rdb.Decr(ctx, "counter").Result()
	if err != nil || n != 9 {
		t.Fatalf("DECR: want 9, got %d err=%v", n, err)
	}
}

func TestIncrNonInteger_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "k", "notanint", 0)
	err := rdb.Incr(ctx, "k").Err()
	if err == nil {
		t.Fatal("INCR non-integer: expected error")
	}
}

func TestAppend_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "greeting", "Hello", 0)
	n, err := rdb.Append(ctx, "greeting", ", World").Result()
	if err != nil || n != 12 {
		t.Fatalf("APPEND: want length 12, got %d err=%v", n, err)
	}
	got, _ := rdb.Get(ctx, "greeting").Result()
	if got != "Hello, World" {
		t.Fatalf("APPEND result: want 'Hello, World', got %q", got)
	}
}
