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

// startServerAt starts the server binary in dir on port, waits until ready,
// and returns the running Cmd. The caller is responsible for killing it.
func startServerAt(t *testing.T, dir, port string) *exec.Cmd {
	t.Helper()
	srv := exec.Command(serverBin, "-port", port)
	srv.Dir = dir
	srv.Stdout = os.Stderr
	srv.Stderr = os.Stderr
	if err := srv.Start(); err != nil {
		t.Fatalf("start server on port %s: %v", port, err)
	}
	addr := "localhost:" + port
	for range 30 {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return srv
		}
		time.Sleep(100 * time.Millisecond)
	}
	srv.Process.Kill()
	t.Fatalf("server on port %s did not become ready", port)
	return nil
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

// Stage 7 — LPUSH / RPUSH / LPOP / RPOP / LLEN / LRANGE

func TestRPush_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	n, err := rdb.RPush(ctx, "mylist", "a", "b", "c").Result()
	if err != nil || n != 3 {
		t.Fatalf("RPUSH: want 3, got %d err=%v", n, err)
	}
}

func TestLRangeAll_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	items, err := rdb.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil || len(items) != 3 || items[0] != "a" || items[2] != "c" {
		t.Fatalf("LRANGE 0 -1: want [a b c], got %v err=%v", items, err)
	}
}

func TestLPush_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	n, err := rdb.LPush(ctx, "mylist", "z").Result()
	if err != nil || n != 4 {
		t.Fatalf("LPUSH: want 4, got %d err=%v", n, err)
	}
	items, _ := rdb.LRange(ctx, "mylist", 0, -1).Result()
	if items[0] != "z" {
		t.Fatalf("LPUSH: want z at head, got %v", items)
	}
}

func TestLPop_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	val, err := rdb.LPop(ctx, "mylist").Result()
	if err != nil || val != "a" {
		t.Fatalf("LPOP: want a, got %q err=%v", val, err)
	}
}

func TestRPop_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	val, err := rdb.RPop(ctx, "mylist").Result()
	if err != nil || val != "c" {
		t.Fatalf("RPOP: want c, got %q err=%v", val, err)
	}
}

func TestLLen_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	rdb.LPop(ctx, "mylist")
	n, err := rdb.LLen(ctx, "mylist").Result()
	if err != nil || n != 2 {
		t.Fatalf("LLEN: want 2, got %d err=%v", n, err)
	}
}

func TestLRangeNegativeIndex_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.RPush(ctx, "mylist", "a", "b", "c")
	items, err := rdb.LRange(ctx, "mylist", 0, 0).Result()
	if err != nil || len(items) != 1 || items[0] != "a" {
		t.Fatalf("LRANGE 0 0: want [a], got %v err=%v", items, err)
	}
}

func TestLPopEmpty_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	err := rdb.LPop(ctx, "nosuchlist").Err()
	if err != redis.Nil {
		t.Fatalf("LPOP empty: want redis.Nil, got %v", err)
	}
}

func TestWrongType_Stage7(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "strkey", "hello", 0)
	err := rdb.LPush(ctx, "strkey", "x").Err()
	if err == nil {
		t.Fatal("LPUSH on string key: expected WRONGTYPE error")
	}
}

// Stage 8 — AOF persistence

func TestPersistenceAfterRestart_Stage8(t *testing.T) {
	dir := t.TempDir()

	srv := startServerAt(t, dir, "6398")

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6398"})
	rdb.Set(ctx, "foo", "bar", 0)
	rdb.Set(ctx, "counter", "0", 0)
	rdb.Incr(ctx, "counter")
	rdb.RPush(ctx, "mylist", "x", "y", "z")
	rdb.Close()

	srv.Process.Kill()
	srv.Wait()

	srv2 := startServerAt(t, dir, "6398")
	defer srv2.Wait()
	defer srv2.Process.Kill()

	rdb2 := redis.NewClient(&redis.Options{Addr: "localhost:6398"})
	defer rdb2.Close()

	if v, err := rdb2.Get(ctx, "foo").Result(); err != nil || v != "bar" {
		t.Fatalf("foo after restart: want bar, got %q err=%v", v, err)
	}
	if v, err := rdb2.Get(ctx, "counter").Result(); err != nil || v != "1" {
		t.Fatalf("counter after restart: want 1, got %q err=%v", v, err)
	}
	items, err := rdb2.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil || len(items) != 3 || items[0] != "x" || items[2] != "z" {
		t.Fatalf("mylist after restart: want [x y z], got %v err=%v", items, err)
	}
}

func TestReadOnlyCommandsNotPersisted_Stage8(t *testing.T) {
	dir := t.TempDir()

	srv := startServerAt(t, dir, "6397")
	defer srv.Process.Kill()
	defer srv.Wait()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6397"})
	defer rdb.Close()

	rdb.Set(ctx, "k", "v", 0)
	rdb.Get(ctx, "k")
	rdb.Exists(ctx, "k")
	rdb.Keys(ctx, "*")
	rdb.TTL(ctx, "k")

	// Kill and restart; if GET/EXISTS/KEYS/TTL were replayed they would error
	// (wrong arg count or type), so a clean restart proves they weren't logged.
	srv.Process.Kill()
	srv.Wait()

	srv2 := startServerAt(t, dir, "6397")
	defer srv2.Wait()
	defer srv2.Process.Kill()

	rdb2 := redis.NewClient(&redis.Options{Addr: "localhost:6397"})
	defer rdb2.Close()

	if v, err := rdb2.Get(ctx, "k").Result(); err != nil || v != "v" {
		t.Fatalf("k after restart: want v, got %q err=%v", v, err)
	}
}

// Stage 6 — missing DECRBY

func TestDecrBy_Stage6(t *testing.T) {
	rdb := newClient(t)
	flush(t, rdb)

	rdb.Set(ctx, "counter", "10", 0)
	n, err := rdb.DecrBy(ctx, "counter", 3).Result()
	if err != nil || n != 7 {
		t.Fatalf("DECRBY: want 7, got %d err=%v", n, err)
	}
}

// Stage 9 — RDB: BGSAVE / LASTSAVE

func TestBGSave_Stage9(t *testing.T) {
	rdb := newClient(t)
	err := rdb.Do(ctx, "BGSAVE").Err()
	if err != nil {
		t.Fatalf("BGSAVE: unexpected error: %v", err)
	}
}

func TestLastSave_Stage9(t *testing.T) {
	rdb := newClient(t)
	// LASTSAVE returns a Unix timestamp; it must be a positive integer.
	ts, err := rdb.Do(ctx, "LASTSAVE").Int64()
	if err != nil || ts <= 0 {
		t.Fatalf("LASTSAVE: want positive timestamp, got %d err=%v", ts, err)
	}
}

// Stage 10 — Pub/Sub: SUBSCRIBE / PUBLISH / UNSUBSCRIBE
//
// go-redis v9's ReceiveMessage skips subscription confirmations and blocks
// until an actual "message" arrives. The correct pattern is:
//   1. ps.Receive(ctx)      — consume the subscription confirmation
//   2. goroutine calling ps.ReceiveMessage(ctx) — wait for the real message
//   3. pub.Publish(...)     — trigger delivery from the main goroutine
//   4. select on goroutine result with a timeout

func pubSubReceive(t *testing.T, ps *redis.PubSub) <-chan *redis.Message {
	t.Helper()
	ch := make(chan *redis.Message, 1)
	go func() {
		msg, err := ps.ReceiveMessage(context.Background())
		if err == nil {
			ch <- msg
		}
	}()
	return ch
}

func TestPubSubBasic_Stage10(t *testing.T) {
	// Use the test name as the channel to avoid interference from stale
	// subscriptions left by a previous test run that was forcibly killed.
	channel := t.Name()
	sub := newClient(t)
	pub := newClient(t)

	ps := sub.Subscribe(ctx, channel)
	defer ps.Close()

	// ps.Receive blocks until the subscription confirmation arrives.
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe confirmation: %v", err)
	}

	// Arm the receiver goroutine before publishing to avoid the race where
	// the message arrives before ReceiveMessage is called.
	received := pubSubReceive(t, ps)

	n, err := pub.Publish(ctx, channel, "hello").Result()
	if err != nil || n != 1 {
		t.Fatalf("PUBLISH: want 1 receiver, got %d err=%v", n, err)
	}

	select {
	case msg := <-received:
		if msg.Channel != channel || msg.Payload != "hello" {
			t.Fatalf("message: want {%s,hello}, got {%q,%q}", channel, msg.Channel, msg.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestPubSubNoSubscribers_Stage10(t *testing.T) {
	pub := newClient(t)
	n, err := pub.Publish(ctx, "emptychannel", "msg").Result()
	if err != nil || n != 0 {
		t.Fatalf("PUBLISH to empty channel: want 0, got %d err=%v", n, err)
	}
}

func TestPubSubMultipleSubscribers_Stage10(t *testing.T) {
	channel := t.Name()
	sub1 := newClient(t)
	sub2 := newClient(t)
	pub := newClient(t)

	ps1 := sub1.Subscribe(ctx, channel)
	defer ps1.Close()
	ps2 := sub2.Subscribe(ctx, channel)
	defer ps2.Close()

	// Wait for both subscription confirmations before arming receivers.
	if _, err := ps1.Receive(ctx); err != nil {
		t.Fatalf("sub1 confirm: %v", err)
	}
	if _, err := ps2.Receive(ctx); err != nil {
		t.Fatalf("sub2 confirm: %v", err)
	}

	r1 := pubSubReceive(t, ps1)
	r2 := pubSubReceive(t, ps2)

	n, err := pub.Publish(ctx, channel, "goal").Result()
	if err != nil || n != 2 {
		t.Fatalf("PUBLISH to 2 subscribers: want 2, got %d err=%v", n, err)
	}

	timeout := time.After(5 * time.Second)
	for _, ch := range []<-chan *redis.Message{r1, r2} {
		select {
		case msg := <-ch:
			if msg.Payload != "goal" {
				t.Fatalf("subscriber: want payload=goal, got %q", msg.Payload)
			}
		case <-timeout:
			t.Fatal("timed out waiting for subscriber")
		}
	}
}

func TestPubSubMultipleChannels_Stage10(t *testing.T) {
	ch1 := t.Name() + ":1"
	ch2 := t.Name() + ":2"
	sub := newClient(t)
	pub := newClient(t)

	ps := sub.Subscribe(ctx, ch1, ch2)
	defer ps.Close()

	// Consume both subscription confirmations before arming the receiver.
	for range 2 {
		if _, err := ps.Receive(ctx); err != nil {
			t.Fatalf("channel confirm: %v", err)
		}
	}

	msgs := make(chan *redis.Message, 2)
	go func() {
		for range 2 {
			msg, err := ps.ReceiveMessage(context.Background())
			if err == nil {
				msgs <- msg
			}
		}
	}()

	pub.Publish(ctx, ch1, "one")
	pub.Publish(ctx, ch2, "two")

	received := map[string]string{}
	timeout := time.After(5 * time.Second)
	for range 2 {
		select {
		case msg := <-msgs:
			received[msg.Channel] = msg.Payload
		case <-timeout:
			t.Fatal("timed out waiting for messages")
		}
	}
	if received[ch1] != "one" || received[ch2] != "two" {
		t.Fatalf("wrong messages: %v", received)
	}
}

func TestPubSubUnsubscribe_Stage10(t *testing.T) {
	channel := t.Name()
	sub := newClient(t)
	pub := newClient(t)

	ps := sub.Subscribe(ctx, channel)
	defer ps.Close()

	// Confirm subscription.
	if _, err := ps.Receive(ctx); err != nil {
		t.Fatalf("subscribe confirm: %v", err)
	}

	// Unsubscribe and consume the unsubscribe confirmation.
	ps.Unsubscribe(ctx, channel)
	ps.Receive(ctx)

	// Give the server time to process the unsubscription.
	time.Sleep(50 * time.Millisecond)

	n, err := pub.Publish(ctx, channel, "late").Result()
	if err != nil || n != 0 {
		t.Fatalf("PUBLISH after unsubscribe: want 0 receivers, got %d err=%v", n, err)
	}
}
