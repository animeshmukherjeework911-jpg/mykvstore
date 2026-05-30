package store_test

import (
	"path/filepath"
	"testing"

	"mykvstore/internal/aof"
	"mykvstore/internal/handler"
	"mykvstore/internal/store"
)

func replayApply(s *store.Store) func([]string) {
	return func(parts []string) { handler.Dispatch(nil, nil, parts, s, nil, nil) }
}

func tempAOFPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "aof.log")
}

func TestAOFReplayEmpty(t *testing.T) {
	a, err := aof.OpenAOF(tempAOFPath(t))
	if err != nil {
		t.Fatalf("OpenAOF: %v", err)
	}
	defer a.Close()
	if err := a.Replay(replayApply(store.NewStore())); err != nil {
		t.Fatalf("Replay empty AOF: %v", err)
	}
}

func TestAOFReplaySet(t *testing.T) {
	path := tempAOFPath(t)

	a, _ := aof.OpenAOF(path)
	a.Write([]string{"SET", "foo", "bar"})
	a.Write([]string{"SET", "num", "42"})
	a.Close()

	a2, _ := aof.OpenAOF(path)
	defer a2.Close()
	s := store.NewStore()
	if err := a2.Replay(replayApply(s)); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if v, ok := s.Get("foo"); !ok || v != "bar" {
		t.Fatalf("foo: want bar, got %q ok=%v", v, ok)
	}
	if v, ok := s.Get("num"); !ok || v != "42" {
		t.Fatalf("num: want 42, got %q ok=%v", v, ok)
	}
}

func TestAOFReplayList(t *testing.T) {
	path := tempAOFPath(t)

	a, _ := aof.OpenAOF(path)
	a.Write([]string{"RPUSH", "mylist", "x", "y", "z"})
	a.Close()

	a2, _ := aof.OpenAOF(path)
	defer a2.Close()
	s := store.NewStore()
	a2.Replay(replayApply(s))

	items, err := s.LRange("mylist", 0, -1)
	if err != nil || len(items) != 3 || items[0] != "x" || items[2] != "z" {
		t.Fatalf("mylist: want [x y z], got %v err=%v", items, err)
	}
}

func TestAOFReplayIncr(t *testing.T) {
	path := tempAOFPath(t)

	a, _ := aof.OpenAOF(path)
	a.Write([]string{"SET", "counter", "0"})
	a.Write([]string{"INCR", "counter"})
	a.Write([]string{"INCR", "counter"})
	a.Close()

	a2, _ := aof.OpenAOF(path)
	defer a2.Close()
	s := store.NewStore()
	a2.Replay(replayApply(s))

	if v, ok := s.Get("counter"); !ok || v != "2" {
		t.Fatalf("counter: want 2, got %q ok=%v", v, ok)
	}
}

func TestAOFReplayDel(t *testing.T) {
	path := tempAOFPath(t)

	a, _ := aof.OpenAOF(path)
	a.Write([]string{"SET", "k", "v"})
	a.Write([]string{"DEL", "k"})
	a.Close()

	a2, _ := aof.OpenAOF(path)
	defer a2.Close()
	s := store.NewStore()
	a2.Replay(replayApply(s))

	if _, ok := s.Get("k"); ok {
		t.Fatal("k should be absent after DEL replay")
	}
}

// TestAOFAppendAcrossReopens verifies that a reopened AOF appends correctly
// and a second replay sees all commands from both sessions.
func TestAOFAppendAcrossReopens(t *testing.T) {
	path := tempAOFPath(t)

	a, _ := aof.OpenAOF(path)
	a.Write([]string{"SET", "a", "1"})
	a.Close()

	a2, _ := aof.OpenAOF(path)
	a2.Write([]string{"SET", "b", "2"})
	a2.Close()

	a3, _ := aof.OpenAOF(path)
	defer a3.Close()
	s := store.NewStore()
	a3.Replay(replayApply(s))

	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatalf("a: want 1, got %q ok=%v", v, ok)
	}
	if v, ok := s.Get("b"); !ok || v != "2" {
		t.Fatalf("b: want 2, got %q ok=%v", v, ok)
	}
}
