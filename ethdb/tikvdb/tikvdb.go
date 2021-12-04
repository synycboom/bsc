// Package tikvdb implements the key-value database layer based on Tikv.
package tikvdb

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// degradationWarnInterval specifies how often warning should be printed if the
	// leveldb database cannot keep up with requested writes.
	degradationWarnInterval = time.Minute

	// minCache is the minimum amount of memory in megabytes to allocate to leveldb
	// read and write caching, split half and half.
	minCache = 16

	// metricsGatheringInterval specifies the interval to retrieve leveldb database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	fn string            // filename for reporting
	db *tikv.RawKVClient // LevelDB instance

	compTimeMeter      metrics.Meter // Meter for measuring the total time spent in database compaction
	compReadMeter      metrics.Meter // Meter for measuring the data read during compaction
	compWriteMeter     metrics.Meter // Meter for measuring the data written during compaction
	writeDelayNMeter   metrics.Meter // Meter for measuring the write delay number due to database compaction
	writeDelayMeter    metrics.Meter // Meter for measuring the write delay duration due to database compaction
	diskSizeGauge      metrics.Gauge // Gauge for tracking the size of all the levels in the database
	diskReadMeter      metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter     metrics.Meter // Meter for measuring the effective amount of data written
	memCompGauge       metrics.Gauge // Gauge for tracking the number of memory compaction
	level0CompGauge    metrics.Gauge // Gauge for tracking the number of table compaction in level0
	nonlevel0CompGauge metrics.Gauge // Gauge for tracking the number of table compaction in non0 level
	seekCompGauge      metrics.Gauge // Gauge for tracking the number of table compaction caused by read opt

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped Tikv object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, namespace string, pdAddrs []string) (*Database, error) {
	return NewCustom(file, namespace, pdAddrs)
}

// NewCustom returns a wrapped Tikv object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
// The customize function allows the caller to modify the tikv options.
func NewCustom(file string, namespace string, pdAddrs []string) (*Database, error) {
	logger := log.New("database", file)
	db, err := tikv.NewRawKVClient(pdAddrs, config.DefaultConfig().Security)
	if err != nil {
		return nil, err
	}

	// Assemble the wrapper with all the registered metrics
	ldb := &Database{
		fn:       file,
		db:       db,
		log:      logger,
		quitChan: make(chan chan error),
	}
	ldb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	ldb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	ldb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	ldb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	ldb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	ldb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	ldb.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	ldb.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	ldb.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	ldb.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	ldb.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	ldb.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)

	// Start up the metrics gathering and return
	// go ldb.meter(metricsGatheringInterval)
	return ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("Metrics collection failed", "err", err)
		}

		db.quitChan = nil
	}

	return db.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	val, err := db.db.Get(key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	val, err := db.db.Get(key)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.db.Put(key, value)
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.db.Delete(key)
}

// Compact is implemented
func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}
