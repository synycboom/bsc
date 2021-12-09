package tikvdb

import (
	"bytes"

	"github.com/ethereum/go-ethereum/ethdb"
)

type tikvdbIterator interface {
	Valid() bool
	Key() []byte
	Value() []byte
	Next() error
	Close()
}

// txnIterator can walk over the (potentially partial) keyspace of a memory key
// value store. Internally it is a deep copy of the entire iterated state,
// sorted by keys.
type txnIterator struct {
	scanner     tikvdbIterator
	err         error
	isFirstCall bool
	start       []byte
	prefix      []byte
	fullPrefix  []byte
}

func (db *Database) newTxnIterator(prefix []byte, start []byte) ethdb.Iterator {
	txn, err := db.txnDB.Begin()
	if err != nil {
		panic(err)
	}

	scanner, err := txn.Iter(prefix, nil)
	if err != nil {
		panic(err)
	}

	return &txnIterator{
		// TODO: Handle iteration error
		err:         err,
		isFirstCall: true,
		fullPrefix:  bytes.Join([][]byte{prefix, start}, nil),
		prefix:      prefix,
		scanner:     scanner,
		start:       start,
	}
}

func (it *txnIterator) seek() (bool, error) {
	for it.scanner.Valid() {
		key := it.scanner.Key()
		if bytes.HasPrefix(key, it.prefix) && bytes.Compare(key, it.fullPrefix) >= 0 {
			return true, nil
		}

		if err := it.scanner.Next(); err != nil {
			return false, err
		}
	}

	return false, nil
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (it *txnIterator) Next() bool {
	if it.isFirstCall {
		it.isFirstCall = false
	} else {
		if err := it.scanner.Next(); err != nil {
			panic(err)
		}
	}

	isValid, err := it.seek()
	if err != nil {
		panic(err)
	}

	return isValid
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *txnIterator) Error() error {
	return nil
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *txnIterator) Key() []byte {
	return it.scanner.Key()
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *txnIterator) Value() []byte {
	return it.scanner.Value()
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *txnIterator) Release() {
	it.scanner.Close()
}
