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
	"path"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"

	badger "github.com/dgraph-io/badger/v3"
)

const (
	// metricsGCInterval specifies the interval to retrieve badger database run GC
	metricsGCInterval = 5 * time.Minute
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	db *badger.DB // Badger instance

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped Badger object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(file string, namespace string, readonly bool) (*Database, error) {
	// TODO: more custom options
	dir := path.Join(file, "db")
	valueDir := path.Join(file, "value-db")
	opts := badger.DefaultOptions(file).WithDir(dir).WithValueDir(valueDir)
	if readonly {
		opts = opts.WithReadOnly(true)
	}
	logger := log.New("database", "badgerdb")
	logCtx := []interface{}{"path", file}
	if opts.ReadOnly {
		logCtx = append(logCtx, "readonly", "true")
	}
	logger.Info("Allocated BadgerDB", logCtx...)

	// Open the db and recover any potential corruptions
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	bdb := &Database{
		db:       db,
		log:      logger,
		quitChan: make(chan chan error),
	}

	go bdb.runGC(metricsGCInterval)

	return bdb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.log.Info("Close database")
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.quitChan != nil {
		errc := make(chan error)
		db.quitChan <- errc
		if err := <-errc; err != nil {
			db.log.Error("GC failed", "err", err)
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
	iter *badger.Iterator
	err  error
}

func (i *iterator) Next() bool {
	i.iter.Next()
	return i.iter.Valid()
}
func (i *iterator) Error() error {
	return i.err
}
func (i *iterator) Key() []byte {
	item := i.iter.Item()
	return item.KeyCopy(nil)
}
func (i *iterator) Value() []byte {
	item := i.iter.Item()
	data, err := item.ValueCopy(nil)
	if err != nil {
		i.err = err
		log.Error("iterator ValueCopy error", "error", err)
	}
	return data
}
func (i *iterator) Release() {
	i.iter.Close()
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	iopt := badger.DefaultIteratorOptions
	iopt.Prefix = append(prefix, start...)
	iter := db.db.NewTransaction(false).NewIterator(iopt)
	iter.Rewind()
	return &iterator{iter: iter}
}

// TODO: fetch badger's inner stats
func (db *Database) Stat(property string) (string, error)     { return "", nil }
func (db *Database) Compact(start []byte, limit []byte) error { return nil }

// runGC is a goroutine that runs Badger ValueGC in long-running
func (db *Database) runGC(interval time.Duration) {
	var (
		errc chan error
		ierr error
	)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for errc == nil && ierr == nil {
		lsm, vlog := db.db.Size()
		db.log.Info("Badger Size", "LSM", lsm, "VLOG", vlog)
		if lsm > 1024*1024*8 || vlog > 1024*1024*32 {
			ierr = db.db.RunValueLogGC(0.5)
			db.log.Error("Badger RunValueLogGC", "error", ierr)
		}

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-db.quitChan:
			// Quit requesting, stop hammering the database
		case <-timer.C:
			timer.Reset(interval)
			// Timeout, gather a new set of stats
		}
	}

	if errc == nil {
		errc = <-db.quitChan
	}
	errc <- ierr
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

// batch is a write-only Badger batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db     *Database
	wb     *badger.WriteBatch
	writes []keyvalue
	size   int
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	wb := db.db.NewWriteBatch()
	wb.SetMaxPendingTxns(128)
	return &batch{db: db, wb: wb}
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
// after Flush, the db.txn is Discard and a new WriteBatch is needed.
func (b *batch) Write() error {
	err := b.wb.Flush()
	b.wb = b.db.db.NewWriteBatch()
	b.wb.SetMaxPendingTxns(128)
	return err
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.wb.Cancel()
	b.wb = b.db.db.NewWriteBatch()
	b.wb.SetMaxPendingTxns(128)
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
