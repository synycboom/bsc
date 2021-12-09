// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tikvdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/dbtest"
)

func TestLevelDB(t *testing.T) {
	var useTxn = true
	var cleanUp = func(db *Database) {
		it := db.NewIterator(nil, nil)
		defer it.Release()

		for it.Next() {
			if err := db.Delete(it.Key()); err != nil {
				panic(err)
			}
		}
	}

	t.Run("DatabaseSuite(txn)", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			db, err := New("", "", []string{"127.0.0.1:2379"}, useTxn)
			if err != nil {
				panic(err)
			}

			cleanUp(db)

			return db
		})
	})

	t.Run("DatabaseSuite(rawKV)", func(t *testing.T) {
		dbtest.TestDatabaseSuite(t, func() ethdb.KeyValueStore {
			db, err := New("", "", []string{"127.0.0.1:2379"}, !useTxn)
			if err != nil {
				panic(err)
			}

			cleanUp(db)

			return db
		})
	})
}
