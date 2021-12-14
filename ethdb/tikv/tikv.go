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

// Package tikv implements the key-value database layer based on the distributed transactional key-value database TiKV
package tikv

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/unionstore"
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	db *txnkv.Client

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

// New returns a wrapped TiKV object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(pdUrls string) (*Database, error) {
	logger := log.New("database", "tikv")
	logCtx := []interface{}{"pd.urls", pdUrls}

	// Open the db and recover any potential corruptions
	db, err := txnkv.NewClient(strings.Split(pdUrls, ","))
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	bdb := &Database{
		db:  db,
		log: logger,
	}
	logger.Info("Open TiKV", logCtx...)

	return bdb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.log.Info("Close database")
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	return db.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	txn, err := db.db.Begin()
	if err != nil {
		return false, err
	}
	_, err = txn.Get(context.Background(), key)
	if err == nil {
		return true, nil
	}
	return !tikverr.IsErrNotFound(err), err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	txn, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	return txn.Get(context.Background(), key)
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	txn, err := db.db.Begin()
	if err != nil {
		return err
	}
	err = txn.Set(key, value)
	if err != nil {
		return err
	}
	return txn.Commit(context.Background())
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	txn, err := db.db.Begin()
	if err != nil {
		return err
	}
	err = txn.Delete(key)
	if err != nil {
		return err
	}
	return txn.Commit(context.Background())
}

type iterator struct {
	txn  *transaction.KVTxn
	iter unionstore.Iterator
	err  error
}

func (i *iterator) Next() bool {
	i.err = i.iter.Next()
	return i.iter.Valid()
}
func (i *iterator) Error() error {
	return i.err
}
func (i *iterator) Key() []byte {
	return i.iter.Key()
}
func (i *iterator) Value() []byte {
	return i.iter.Value()
}
func (i *iterator) Release() {
	i.iter.Close()
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	txn, err := db.db.Begin()
	if err != nil {
		panic(err)
	}
	iter, err := txn.Iter(append(prefix, start...), nil)
	if err != nil {
		panic(err)
	}
	return &iterator{
		txn:  txn,
		iter: iter,
	}
}

// TODO: fetch inner stats
func (db *Database) Stat(property string) (string, error)     { return "", nil }
func (db *Database) Compact(start []byte, limit []byte) error { return nil }

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

// batch is a write-only batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db     *txnkv.Client
	writes []keyvalue
	size   int
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() ethdb.Batch {
	return &batch{
		db:     db.db,
		writes: make([]keyvalue, 0),
	}
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.size += len(value)
	b.writes = append(b.writes, keyvalue{key, value, keyTypeVal})
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	b.size += len(key)
	b.writes = append(b.writes, keyvalue{key, nil, keyTypeDel})
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
// after Flush, the db.txn is Discard and a new WriteBatch is needed.
func (b *batch) Write() error {
	txn, err := b.db.Begin()
	if err != nil {
		return err
	}

	for _, keyvalue := range b.writes {
		switch keyvalue.keyType {
		case keyTypeVal:
			err = txn.Set(keyvalue.key, keyvalue.value)
		case keyTypeDel:
			err = txn.Delete(keyvalue.key)
		}
		if err != nil {
			return err
		}
	}

	return txn.Commit(context.Background())
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
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
