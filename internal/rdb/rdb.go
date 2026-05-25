package rdb

import (
	"fmt"
	"mykvstore/internal/store"
	"os"
	"time"
)

const rdbPath = "dump.rdb"

type Snapshot struct {
	Data    map[string]string
	Expures map[string]time.Time
	Lists   map[string][]string
	SavedAt time.Time
}

func saveRDB(store *store.Store, path string) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("rdb create tmp: %w", err)
	}


}
