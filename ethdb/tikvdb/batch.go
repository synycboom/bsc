package tikvdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

// keyvalue is a key-value tuple tagged with a deletion field to allow creating
// tikvdb write batches.
type keyvalue struct {
	key    []byte
	value  []byte
	delete bool
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	if db.useTxn {
		return db.newTxnBatch()
	}

	return db.newRawKVBatch()
}
