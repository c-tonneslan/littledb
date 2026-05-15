// Package littledb is a tiny embedded key/value store. It stores data
// in a single file using a copy-on-write B+tree, the same family of
// designs as LMDB and BoltDB. The implementation is around a thousand
// lines of Go and exists mostly to teach itself how those databases
// work, not to compete with them.
//
// Quick start:
//
//	db, err := littledb.Open("data.db", 0600)
//	if err != nil { ... }
//	defer db.Close()
//
//	err = db.Update(func(tx *littledb.Tx) error {
//	    return tx.Put([]byte("hello"), []byte("world"))
//	})
//
//	err = db.View(func(tx *littledb.Tx) error {
//	    fmt.Println(string(tx.Get([]byte("hello"))))
//	    return nil
//	})
package littledb

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// DB is a handle to a littledb database. Safe for concurrent use by
// multiple goroutines.
type DB struct {
	path string
	file *os.File

	mu        sync.Mutex // protects writable transactions and metadata
	rmu       sync.RWMutex // read lock acquired by readers, write lock by committers
	meta      meta
	fl        *freelist
	highWater pgid // next page id to allocate beyond the freelist

	openReaders map[*Tx]struct{}
	writableTx  *Tx
	closed      bool
}

// Open opens (or creates) a littledb database at path. Mode is the file
// mode used if the file needs to be created.
func Open(path string, mode os.FileMode) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	db := &DB{
		path:        path,
		file:        f,
		fl:          newFreelist(),
		openReaders: make(map[*Tx]struct{}),
	}
	if st.Size() == 0 {
		if err := db.initialize(); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		if err := db.load(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return db, nil
}

// Close flushes any pending state and closes the underlying file. It is
// an error to use the DB after Close returns.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if err := db.file.Sync(); err != nil {
		db.file.Close()
		return err
	}
	return db.file.Close()
}

// initialize lays down the meta pages and an empty root page on a new file.
func (db *DB) initialize() error {
	// Both meta pages reference an empty tree (root = 0). The freelist
	// is empty and starts at page 2 after we serialize it for the first
	// time during the first transaction.
	db.meta = meta{
		magic:    magicNumber,
		version:  formatVersion,
		pageSize: uint32(pageSize),
		root:     0,
		freelist: 0,
		pgid:     2,
		txid:     0,
	}
	db.highWater = 2

	buf := make([]byte, pageSize)
	db.meta.writeTo(buf)
	if err := db.writePage(metaPgidA, buf); err != nil {
		return err
	}
	db.meta.txid = 1
	buf2 := make([]byte, pageSize)
	db.meta.writeTo(buf2)
	if err := db.writePage(metaPgidB, buf2); err != nil {
		return err
	}
	db.meta.txid = 1
	return db.file.Sync()
}

// load reads the meta pages and picks the most recent valid one.
func (db *DB) load() error {
	bufA := make([]byte, pageSize)
	bufB := make([]byte, pageSize)
	if _, err := db.file.ReadAt(bufA, 0); err != nil {
		return err
	}
	if _, err := db.file.ReadAt(bufB, int64(pageSize)); err != nil {
		return err
	}
	mA, errA := readMeta(bufA)
	mB, errB := readMeta(bufB)
	if errA != nil && errB != nil {
		return errors.Join(errA, errB)
	}
	switch {
	case mA == nil && mB == nil:
		return errBothMetaCorrupt
	case mA == nil:
		db.meta = *mB
	case mB == nil:
		db.meta = *mA
	case mA.txid >= mB.txid:
		db.meta = *mA
	default:
		db.meta = *mB
	}
	db.highWater = db.meta.pgid

	if db.meta.freelist != 0 {
		buf, err := db.readPage(db.meta.freelist)
		if err != nil {
			return fmt.Errorf("littledb: read freelist page %d: %w", db.meta.freelist, err)
		}
		db.fl.readFrom(buf)
	}
	return nil
}

// readPage reads a single page from disk. The returned slice is freshly
// allocated and owned by the caller.
func (db *DB) readPage(id pgid) ([]byte, error) {
	buf := make([]byte, pageSize)
	if _, err := db.file.ReadAt(buf, int64(id)*int64(pageSize)); err != nil {
		return nil, err
	}
	return buf, nil
}

// writePage writes a single page to disk. Does NOT sync.
func (db *DB) writePage(id pgid, buf []byte) error {
	if len(buf) != pageSize {
		return fmt.Errorf("littledb: writePage buffer size %d != %d", len(buf), pageSize)
	}
	if _, err := db.file.WriteAt(buf, int64(id)*int64(pageSize)); err != nil {
		return err
	}
	return nil
}

// Begin starts a transaction. If writable is true, the caller must hold
// exclusive access; only one writable transaction may be in flight at
// any time.
func (db *DB) Begin(writable bool) (*Tx, error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, errors.New("littledb: database closed")
	}
	if writable {
		if db.writableTx != nil {
			db.mu.Unlock()
			return nil, errors.New("littledb: writable transaction already in progress")
		}
	}
	tx := &Tx{
		db:       db,
		meta:     db.meta,
		writable: writable,
		nodes:    make(map[pgid]*node),
	}
	if writable {
		db.writableTx = tx
	} else {
		db.openReaders[tx] = struct{}{}
	}
	db.mu.Unlock()
	return tx, nil
}

// endTx removes a transaction from the open set.
func (db *DB) endTx(tx *Tx) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if tx.writable {
		if db.writableTx == tx {
			db.writableTx = nil
		}
	} else {
		delete(db.openReaders, tx)
	}
}

// minReaderTxid returns the smallest txid still in use by any open
// reader. If no readers are open, returns the current committed txid.
// Pages freed by older transactions are safe to release.
func (db *DB) minReaderTxid() uint64 {
	min := db.meta.txid
	for r := range db.openReaders {
		if r.meta.txid < min {
			min = r.meta.txid
		}
	}
	return min
}

// Update runs fn inside a writable transaction and commits if fn returns
// nil. If fn returns an error or panics, the transaction is rolled back.
func (db *DB) Update(fn func(tx *Tx) error) error {
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// View runs fn inside a read-only transaction.
func (db *DB) View(fn func(tx *Tx) error) error {
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return fn(tx)
}

// Stats reports basic counters about the database. Useful for tests and
// observability.
type Stats struct {
	FreePages    int
	PendingPages int
	HighWater    uint64
	TxID         uint64
	Root         uint64
}

// Stats returns a snapshot of database statistics.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	return Stats{
		FreePages:    db.fl.count(),
		PendingPages: db.fl.pendingCount(),
		HighWater:    uint64(db.highWater),
		TxID:         db.meta.txid,
		Root:         uint64(db.meta.root),
	}
}
