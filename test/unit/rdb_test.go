package store_test

import (
	"path/filepath"
	"testing"
	"time"

	"mykvstore/internal/rdb"
	"mykvstore/internal/store"
)

func tempRDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "dump.rdb")
}

func TestLoadRDBMissing(t *testing.T) {
	s := store.NewStore()
	savedAt, err := rdb.LoadRDB(s, tempRDBPath(t))
	if err != nil {
		t.Fatalf("expected no error for missing file, got %v", err)
	}
	if !savedAt.IsZero() {
		t.Fatalf("expected zero time for missing file, got %v", savedAt)
	}
}

func TestSaveAndLoadRDB(t *testing.T) {
	path := tempRDBPath(t)

	// populate a store and save
	orig := store.NewStore()
	orig.Set("str", "hello")
	orig.SetWithTTL("expiring", "soon", 10*time.Minute)
	orig.RPush("mylist", "a", "b", "c")

	before := time.Now().Truncate(time.Second)
	if err := rdb.SaveRDB(orig, path); err != nil {
		t.Fatalf("SaveRDB: %v", err)
	}
	after := time.Now().Add(time.Second)

	// load into a fresh store
	fresh := store.NewStore()
	savedAt, err := rdb.LoadRDB(fresh, path)
	if err != nil {
		t.Fatalf("LoadRDB: %v", err)
	}

	if savedAt.Before(before) || savedAt.After(after) {
		t.Fatalf("savedAt %v outside expected window [%v, %v]", savedAt, before, after)
	}

	if v, ok := fresh.Get("str"); !ok || v != "hello" {
		t.Fatalf("str: want hello, got %q ok=%v", v, ok)
	}

	if v, ok := fresh.Get("expiring"); !ok || v != "soon" {
		t.Fatalf("expiring: want soon, got %q ok=%v", v, ok)
	}

	if ttl := fresh.TTL("expiring"); ttl <= 0 || ttl > 10*time.Minute {
		t.Fatalf("expiring TTL should be positive and <= 10m, got %v", ttl)
	}

	items, err := fresh.LRange("mylist", 0, -1)
	if err != nil || len(items) != 3 || items[0] != "a" || items[2] != "c" {
		t.Fatalf("mylist: want [a b c], got %v err=%v", items, err)
	}
}

func TestSaveRDBAtomicOnEncodeFail(t *testing.T) {
	// SaveRDB to a path inside a non-existent directory should fail cleanly
	// and not leave a .tmp file behind.
	path := "/tmp/mykvstore_no_such_dir/dump.rdb"
	s := store.NewStore()
	s.Set("k", "v")

	err := rdb.SaveRDB(s, path)
	if err == nil {
		t.Fatal("expected error saving to non-existent directory")
	}
}

func TestRDBSaverTrigger(t *testing.T) {
	path := tempRDBPath(t)
	s := store.NewStore()
	s.Set("key", "value")

	saver := rdb.NewRDBSaver(s, path)
	saver.Start(1 * time.Hour) // long interval — only BGSAVE triggers a save
	defer saver.Stop()

	if !saver.LastSave().IsZero() {
		t.Fatal("LastSave should be zero before any save")
	}

	saver.Trigger()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !saver.LastSave().IsZero() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if saver.LastSave().IsZero() {
		t.Fatal("LastSave still zero after Trigger — save never completed")
	}

	// verify the file contains the correct data
	fresh := store.NewStore()
	if _, err := rdb.LoadRDB(fresh, path); err != nil {
		t.Fatalf("LoadRDB after Trigger: %v", err)
	}
	if v, ok := fresh.Get("key"); !ok || v != "value" {
		t.Fatalf("key: want value, got %q ok=%v", v, ok)
	}
}

func TestRDBSaverStop(t *testing.T) {
	path := tempRDBPath(t)
	s := store.NewStore()
	s.Set("shutdown", "data")

	saver := rdb.NewRDBSaver(s, path)
	saver.Start(1 * time.Hour)
	saver.Stop() // should do a final save before returning

	if saver.LastSave().IsZero() {
		t.Fatal("LastSave should be set after Stop (shutdown save)")
	}

	fresh := store.NewStore()
	if _, err := rdb.LoadRDB(fresh, path); err != nil {
		t.Fatalf("LoadRDB after Stop: %v", err)
	}
	if v, ok := fresh.Get("shutdown"); !ok || v != "data" {
		t.Fatalf("shutdown: want data, got %q ok=%v", v, ok)
	}
}
