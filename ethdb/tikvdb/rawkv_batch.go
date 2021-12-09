package tikvdb

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tikv/client-go/v2/rawkv"
)

type rawkvBatch struct {
	db     *rawkv.Client
	lock   sync.RWMutex
	writes []keyvalue
	size   int
}

func (db *Database) newRawKVBatch() ethdb.Batch {
	return &rawkvBatch{
		db: db.db,
	}
}

// Put inserts the given value into the batch for later committing.
func (b *rawkvBatch) Put(key, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	v := value
	if v == nil {
		v = []byte("\x00")
	}
	rkv := keyvalue{common.CopyBytes(key), common.CopyBytes(v), false}
	b.writes = append(b.writes, rkv)
	b.size += len(value)

	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *rawkvBatch) Delete(key []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	rkv := keyvalue{common.CopyBytes(key), nil, true}
	b.writes = append(b.writes, rkv)
	b.size += len(key)

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *rawkvBatch) ValueSize() int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.size
}

// Write flushes any accumulated data to the tikv database.
func (b *rawkvBatch) Write() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	ctx := context.Background()
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

	deleteKeys := make([][]byte, 0, len(deleteSet))
	for key := range deleteSet {
		deleteKeys = append(deleteKeys, []byte(key))
	}
	if err := b.db.BatchDelete(ctx, deleteKeys); err != nil {
		return err
	}

	putKeys := make([][]byte, 0, len(putSet))
	putValues := make([][]byte, 0, len(putSet))
	for key, val := range putSet {
		putKeys = append(putKeys, []byte(key))
		putValues = append(putValues, val)
	}
	if err := b.db.BatchPut(ctx, putKeys, putValues, nil); err != nil {
		return err
	}

	return nil
}

// Reset resets the batch for reuse.
func (b *rawkvBatch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.writes = b.writes[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *rawkvBatch) Replay(w ethdb.KeyValueWriter) error {
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
