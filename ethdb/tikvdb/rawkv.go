package tikvdb

import "context"

func (db *Database) rawKVHas(key []byte) (bool, error) {
	key, err := db.db.Get(context.Background(), key)
	if err != nil {
		return false, err
	}

	return key != nil, nil
}

func (db *Database) rawKVGet(key []byte) ([]byte, error) {
	return db.db.Get(context.Background(), key)
}

func (db *Database) rawKVPut(key []byte, value []byte) error {
	v := value
	if v == nil {
		v = []byte("\x00")
	}

	return db.db.Put(context.Background(), key, v)
}

func (db *Database) rawKVDelete(key []byte) error {
	return db.db.Delete(context.Background(), key)
}
