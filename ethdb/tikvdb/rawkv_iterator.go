package tikvdb

import (
	"bytes"
	"context"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tikv/client-go/v2/rawkv"
)

const (
	// TODO: Tune up
	ScanLimit = 2
)

type rawkvIterator struct {
	db         *rawkv.Client
	err        error
	prefix     []byte
	fullPrefix []byte
	minKey     []byte
	idx        int
	currKeys   [][]byte
	currVals   [][]byte
}

func (db *Database) newRawKVIterator(prefix []byte, start []byte) ethdb.Iterator {
	return &rawkvIterator{
		err:        nil,
		fullPrefix: bytes.Join([][]byte{prefix, start}, nil),
		prefix:     prefix,
		db:         db.db,
		minKey:     prefix,
		idx:        -1,
	}
}

func (it *rawkvIterator) seek() (bool, error) {
	ctx := context.Background()
	for {
		it.idx += 1

		if it.idx >= len(it.currKeys) {
			keys, values, err := it.db.Scan(ctx, it.minKey, nil, ScanLimit)
			if err != nil {
				return false, err
			}

			it.currKeys = keys
			it.currVals = values
			it.idx = 0
			if len(keys) > 0 {
				it.minKey = bytes.Join([][]byte{keys[len(keys)-1], []byte("\x00")}, nil)
			}
		}

		if len(it.currKeys) == 0 {
			return false, nil
		}

		key := it.currKeys[it.idx]
		if bytes.HasPrefix(key, it.prefix) && bytes.Compare(key, it.fullPrefix) >= 0 {
			return true, nil
		}
	}
}

// Next moves the iterator to the next key/value pair. It returns whether the
// iterator is exhausted.
func (it *rawkvIterator) Next() bool {
	hasMoreData, err := it.seek()
	if err != nil {
		it.err = err
		return false
	}

	return hasMoreData
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *rawkvIterator) Error() error {
	return it.err
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *rawkvIterator) Key() []byte {
	return it.currKeys[it.idx]
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *rawkvIterator) Value() []byte {
	return it.currVals[it.idx]
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *rawkvIterator) Release() {
	// TODO: verify
	it.currKeys = nil
	it.currVals = nil
}
