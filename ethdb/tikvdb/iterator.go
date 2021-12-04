package tikvdb

import (
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tikv/client-go/v2/tikv"
)

// iterator can walk over the (potentially partial) keyspace of a memory key
// value store. Internally it is a deep copy of the entire iterated state,
// sorted by keys.
type iterator struct {
	db         *tikv.RawKVClient
	lock       sync.RWMutex
	prefix     []byte
	start      []byte
	nextPrefix []byte
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	return &iterator{
		db:     db.db,
		prefix: prefix,
		start:  start,
	}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (it *iterator) Next() bool {
	// it.db.Scan()
	// // If the iterator was not yet initialized, do it now
	// if !it.inited {
	// 	it.inited = true
	// 	return len(it.keys) > 0
	// }
	// // Iterator already initialize, advance it
	// if len(it.keys) > 0 {
	// 	it.keys = it.keys[1:]
	// 	it.values = it.values[1:]
	// }
	// return len(it.keys) > 0
	return false
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *iterator) Error() error {
	return nil
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *iterator) Key() []byte {
	// if len(it.keys) > 0 {
	// 	return []byte(it.keys[0])
	// }
	return nil
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *iterator) Value() []byte {
	// if len(it.values) > 0 {
	// 	return it.values[0]
	// }
	return nil
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *iterator) Release() {
	// it.keys, it.values = nil, nil
}
