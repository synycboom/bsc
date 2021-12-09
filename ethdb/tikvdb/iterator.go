package tikvdb

import (
	"github.com/ethereum/go-ethereum/ethdb"
)

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	if db.useTxn {
		return db.newTxnIterator(prefix, start)
	}

	return db.newRawKVIterator(prefix, start)
}
