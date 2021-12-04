package tikvdb

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tikv/client-go/v2/tikv"
)

// keyvalue is a key-value tuple tagged with a deletion field to allow creating
// tikvdb write batches.
type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

// batch is a write-only memory batch that commits changes to its host
// database when Write is called. A batch cannot be used concurrently.
type batch struct {
	db     *tikv.RawKVClient
	lock   sync.RWMutex
	writes []keyvalue
	size   int
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	return &batch{
		db: db.db,
	}
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.writes = append(b.writes, keyvalue{common.CopyBytes(key), common.CopyBytes(value), false})
	b.size += len(value)

	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.writes = append(b.writes, keyvalue{common.CopyBytes(key), nil, true})
	b.size += len(key)

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.size
}

// Write flushes any accumulated data to the tikv database.
func (b *batch) Write() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	deleteSet := make(map[string]struct{})
	putSet := make(map[string][]byte)
	for i := len(b.writes) - 1; i >= 0; i-- {
		key := string(b.writes[i].key)
		val := b.writes[i].value
		if _, ok := deleteSet[key]; ok {
			continue
		}
		if _, ok := putSet[key]; ok {
			continue
		}

		if b.writes[i].delete {
			deleteSet[key] = struct{}{}
		} else {
			putSet[key] = val
		}
	}

	deleteKeys := make([][]byte, len(deleteSet), 0)
	for key := range deleteSet {
		deleteKeys = append(deleteKeys, []byte(key))
	}
	if err := b.db.BatchDelete(deleteKeys); err != nil {
		return err
	}

	putKeys := make([][]byte, len(putSet), 0)
	putValues := make([][]byte, len(putSet), 0)
	for key, val := range putSet {
		putKeys = append(putKeys, []byte(key))
		putValues = append(putValues, val)
	}
	if err := b.db.BatchPut(putKeys, putValues); err != nil {
		return err
	}

	return nil
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.writes = b.writes[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	b.lock.RLock()
	defer b.lock.RUnlock()

	for _, keyvalue := range b.writes {
		if keyvalue.delete {
			if err := w.Delete(keyvalue.key); err != nil {
				return err
			}

			continue
		}
		if err := w.Put(keyvalue.key, keyvalue.value); err != nil {
			return err
		}
	}

	return nil
}
