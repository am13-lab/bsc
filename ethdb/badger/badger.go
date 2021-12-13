// Copyright 2021 The go-ethereum Authors
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

// +build !js

// Package badger implements the key-value database layer based on dgraph-io/badger.
package badger

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb/util"

	badger "github.com/dgraph-io/badger/v3"
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	fn string     // filename for reporting
	db *badger.DB // Badger instance

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

// New returns a wrapped Badger object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, namespace string, readonly bool) (*Database, error) {
	// TODO: more custom options
	opts := badger.DefaultOptions(file)
	if readonly {
		opts = opts.WithReadOnly(true)
	}
	logger := log.New("database", file)
	logCtx := []interface{}{"path", file}
	if opts.ReadOnly {
		logCtx = append(logCtx, "readonly", "true")
	}
	logger.Info("Allocated cache and file handles", logCtx...)

	// Open the db and recover any potential corruptions
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	bdb := &Database{
		fn:       file,
		db:       db,
		log:      logger,
		quitChan: make(chan chan error),
	}
	bdb.compTimeMeter = metrics.NewRegisteredMeter(namespace+"compact/time", nil)
	bdb.compReadMeter = metrics.NewRegisteredMeter(namespace+"compact/input", nil)
	bdb.compWriteMeter = metrics.NewRegisteredMeter(namespace+"compact/output", nil)
	bdb.diskSizeGauge = metrics.NewRegisteredGauge(namespace+"disk/size", nil)
	bdb.diskReadMeter = metrics.NewRegisteredMeter(namespace+"disk/read", nil)
	bdb.diskWriteMeter = metrics.NewRegisteredMeter(namespace+"disk/write", nil)
	bdb.writeDelayMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/duration", nil)
	bdb.writeDelayNMeter = metrics.NewRegisteredMeter(namespace+"compact/writedelay/counter", nil)
	bdb.memCompGauge = metrics.NewRegisteredGauge(namespace+"compact/memory", nil)
	bdb.level0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/level0", nil)
	bdb.nonlevel0CompGauge = metrics.NewRegisteredGauge(namespace+"compact/nonlevel0", nil)
	bdb.seekCompGauge = metrics.NewRegisteredGauge(namespace+"compact/seek", nil)

	return bdb, nil
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
	found := false
	err := db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			found = true
		}
		return err
	})
	return found, err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	var dat []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		dat, err = item.ValueCopy(nil)
		return err
	})
	return dat, err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

type iterator struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func (i *iterator) Next() bool {
	return true
}
func (i *iterator) Error() error {
	return nil
}
func (i *iterator) Key() []byte {
	return nil
}
func (i *iterator) Value() []byte {
	return nil
}
func (i *iterator) Release() {
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	txn := db.db.NewTransaction(false)
	iopt := badger.DefaultIteratorOptions
	iopt.AllVersions = true
	iopt.InternalAccess = true
	iter := txn.NewIterator(iopt)
	return &iterator{
		txn:  txn,
		iter: iter,
	}
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return "", nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.fn
}

// stole from leveldb
type keyType uint

func (kt keyType) String() string {
	switch kt {
	case keyTypeDel:
		return "d"
	case keyTypeVal:
		return "v"
	}
	return fmt.Sprintf("<invalid:%#x>", uint(kt))
}

// Value types encoded as the last component of internal keys.
// Don't modify; this value are saved to disk.
const (
	keyTypeDel = keyType(0)
	keyTypeVal = keyType(1)
)

type keyvalue struct {
	key     []byte
	value   []byte
	keyType keyType
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	wb     *badger.WriteBatch
	writes []keyvalue
	size   int
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	wb := db.db.NewWriteBatch()
	wb.SetMaxPendingTxns(64)
	return &batch{wb: wb}
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	err := b.wb.Set(key, value)
	b.size += len(value)
	b.writes = append(b.writes, keyvalue{key, value, keyTypeVal})
	return err
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	err := b.wb.Delete(key)
	b.size += len(key)
	b.writes = append(b.writes, keyvalue{key, nil, keyTypeDel})
	return err
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	return b.wb.Flush()
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.wb.Cancel()
	b.writes = b.writes[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *batch) Replay(w ethdb.KeyValueWriter) error {
	var err error
	for _, keyvalue := range b.writes {
		switch keyvalue.keyType {
		case keyTypeVal:
			err = w.Put(keyvalue.key, keyvalue.value)
		case keyTypeDel:
			err = w.Delete(keyvalue.key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// replayer is a small wrapper to implement the correct replay methods.
type replayer struct {
	writer  ethdb.KeyValueWriter
	failure error
}

// Put inserts the given value into the key-value data store.
func (r *replayer) Put(key, value []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Put(key, value)
}

// Delete removes the key from the key-value data store.
func (r *replayer) Delete(key []byte) {
	// If the replay already failed, stop executing ops
	if r.failure != nil {
		return
	}
	r.failure = r.writer.Delete(key)
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}
