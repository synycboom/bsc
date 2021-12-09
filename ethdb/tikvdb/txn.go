package tikvdb

import (
	"context"

	tikverr "github.com/tikv/client-go/v2/error"
)

func (db *Database) txnHas(key []byte) (bool, error) {
	tx, err := db.txnDB.Begin()
	if err != nil {
		return false, err
	}

	ss := tx.GetSnapshot()
	ss.SetKeyOnly(true)
	_, err = ss.Get(context.Background(), key)
	if tikverr.IsErrNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (db *Database) txnGet(key []byte) ([]byte, error) {
	// TODO: the timestamp has to be optimized
	tx, err := db.txnDB.Begin()
	if err != nil {
		return nil, err
	}

	ss := tx.GetSnapshot()
	ss.SetKeyOnly(true)
	val, err := ss.Get(context.Background(), key)
	if tikverr.IsErrNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *Database) txnPut(key []byte, value []byte) error {
	tx, err := db.txnDB.Begin()
	if err != nil {
		return err
	}

	tx.SetEnable1PC(true)

	v := value
	if v == nil {
		v = []byte("\x00")
	}
	if err := tx.Set(key, v); err != nil {
		return err
	}

	if err := tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}

func (db *Database) txnDelete(key []byte) error {
	tx, err := db.txnDB.Begin()
	if err != nil {
		return err
	}

	tx.SetEnable1PC(true)

	if err := tx.Delete(key); err != nil {
		return err
	}

	if err := tx.Commit(context.Background()); err != nil {
		return err
	}

	return nil
}
