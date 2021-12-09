package tikvdb

import (
	"context"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tikv/client-go/v2/tikv"
)

// txnBatch is a write-only memory txnBatch that commits changes to its host
// database when Write is called. A txnBatch cannot be used concurrently.
type txnBatch struct {
	db     *tikv.KVStore
	lock   sync.RWMutex
	writes []keyvalue
	size   int
}

func (db *Database) newTxnBatch() ethdb.Batch {
	return &txnBatch{
		db: db.txnDB,
	}
}

// Put inserts the given value into the batch for later committing.
func (b *txnBatch) Put(key, value []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	v := value
	if v == nil {
		v = []byte("\x00")
	}

	kv := keyvalue{common.CopyBytes(key), common.CopyBytes(v), false}
	b.writes = append(b.writes, kv)
	b.size += len(value)

	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *txnBatch) Delete(key []byte) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	kv := keyvalue{common.CopyBytes(key), nil, true}
	b.writes = append(b.writes, kv)
	b.size += len(key)

	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *txnBatch) ValueSize() int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	return b.size
}

// Write flushes any accumulated data to the tikv database.
func (b *txnBatch) Write() error {
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

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}

	for key := range deleteSet {
		if err := tx.Delete([]byte(key)); err != nil {
			return err
		}
	}

	for key, val := range putSet {
		if err := tx.Set([]byte(key), val); err != nil {
			return err
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

// Reset resets the batch for reuse.
func (b *txnBatch) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.writes = b.writes[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *txnBatch) Replay(w ethdb.KeyValueWriter) error {
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
