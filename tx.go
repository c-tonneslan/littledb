package littledb

import (
	"bytes"
	"errors"
	"fmt"
)

// Tx is a database transaction. Writable transactions can call Put,
// Delete, and Commit; read-only transactions only call Get and Range
// and must be ended with Rollback.
//
// Only one writable transaction may be open at a time. Any number of
// read-only transactions may run concurrently, including alongside a
// writable transaction (they see the database as of the txid at which
// they began).
type Tx struct {
	db       *DB
	meta     meta
	writable bool
	root     *node // in-memory root for writers; nil for readers
	nodes    map[pgid]*node // cached materialized nodes for this writer
	pending  []pgid // newly-allocated page ids during this tx
	closed   bool
}

var (
	errTxClosed         = errors.New("littledb: transaction already closed")
	errReadOnly         = errors.New("littledb: cannot mutate a read-only transaction")
	errKeyTooLarge      = errors.New("littledb: key too large")
	errValueTooLarge    = errors.New("littledb: value too large")
	errFreelistTooLarge = errors.New("littledb: freelist too large for one page")
)

// Get returns the value associated with key, or nil if not found.
// The returned slice is owned by the transaction; copy it if you need
// the bytes outliving Commit/Rollback.
func (tx *Tx) Get(key []byte) []byte {
	if tx.closed {
		return nil
	}
	if tx.writable && tx.root != nil {
		return tx.getFromTree(tx.root, key)
	}
	return tx.getFromDisk(tx.meta.root, key)
}

func (tx *Tx) getFromTree(n *node, key []byte) []byte {
	if n.isLeaf {
		idx, exact := n.search(key)
		if exact {
			return n.elements[idx].value
		}
		return nil
	}
	child := tx.childForKey(n, key)
	return tx.getFromTree(child, key)
}

// getFromDisk walks the on-disk tree without materializing nodes.
func (tx *Tx) getFromDisk(id pgid, key []byte) []byte {
	if id == 0 {
		return nil
	}
	buf, err := tx.db.readPage(id)
	if err != nil {
		return nil
	}
	p := loadPage(buf)
	if p.flags&leafPage != 0 {
		return findInLeafPage(buf, p, key)
	}
	if p.flags&branchPage != 0 {
		return tx.getFromDisk(branchChildForKey(buf, p, key), key)
	}
	return nil
}

// childForKey returns the in-memory child node that owns key.
// Branch node elements use the standard B+tree invariant: child[i] holds
// keys with key >= elements[i].key (and < elements[i+1].key).
func (tx *Tx) childForKey(n *node, key []byte) *node {
	idx, exact := n.search(key)
	if !exact && idx > 0 {
		idx--
	}
	if idx >= len(n.elements) {
		idx = len(n.elements) - 1
	}
	return tx.loadNode(n.elements[idx].child)
}

// loadNode pulls a page into a node, caching it for this transaction.
func (tx *Tx) loadNode(id pgid) *node {
	if n, ok := tx.nodes[id]; ok {
		return n
	}
	buf, err := tx.db.readPage(id)
	if err != nil {
		panic(fmt.Sprintf("littledb: failed to read page %d: %v", id, err))
	}
	p := loadPage(buf)
	n := loadNode(p, buf)
	n.tx = tx
	tx.nodes[id] = n
	return n
}

// Put inserts or updates a key/value pair.
func (tx *Tx) Put(key, value []byte) error {
	if tx.closed {
		return errTxClosed
	}
	if !tx.writable {
		return errReadOnly
	}
	if len(key) == 0 {
		return errors.New("littledb: empty key")
	}
	if len(key) > maxKeySize {
		return errKeyTooLarge
	}
	if len(value) > maxValueSize {
		return errValueTooLarge
	}

	tx.ensureRoot()
	tx.putInto(tx.root, key, value)
	return nil
}

// Delete removes a key. It's not an error to delete a key that doesn't exist.
func (tx *Tx) Delete(key []byte) error {
	if tx.closed {
		return errTxClosed
	}
	if !tx.writable {
		return errReadOnly
	}
	if tx.root == nil {
		tx.ensureRoot()
	}
	tx.deleteFrom(tx.root, key)
	return nil
}

func (tx *Tx) ensureRoot() {
	if tx.root != nil {
		return
	}
	if tx.meta.root == 0 {
		// empty tree, create a fresh leaf
		tx.root = &node{isLeaf: true, dirty: true, tx: tx}
		return
	}
	tx.root = tx.loadNode(tx.meta.root)
	tx.root.dirty = true
}

func (tx *Tx) putInto(n *node, key, value []byte) {
	n.dirty = true
	if n.isLeaf {
		n.put(key, value, 0)
		return
	}
	child := tx.childForKey(n, key)
	child.parent = n
	tx.putInto(child, key, value)
}

func (tx *Tx) deleteFrom(n *node, key []byte) {
	n.dirty = true
	if n.isLeaf {
		n.del(key)
		return
	}
	child := tx.childForKey(n, key)
	child.parent = n
	tx.deleteFrom(child, key)
}

// Commit flushes the transaction's changes to disk and ends the transaction.
// Read-only transactions should call Rollback instead.
func (tx *Tx) Commit() error {
	if tx.closed {
		return errTxClosed
	}
	if !tx.writable {
		return errReadOnly
	}
	defer tx.close()

	if tx.root == nil {
		// nothing changed
		return nil
	}

	tx.meta.txid++
	// Recursively split oversize nodes, serialize bottom-up, and allocate
	// fresh pgids on the way up so we never overwrite a live page. The
	// result is a list of (key, child-pgid) pairs that should replace
	// the root. If there's more than one, we wrap them in a new root.
	pieces, err := tx.spill(tx.root)
	if err != nil {
		return err
	}
	// If the root spill produced more pieces than fit in one page, we
	// have to add another level. Keep wrapping until we collapse to a
	// single root. This terminates because each iteration reduces the
	// page count by at least a factor of (pageSize / max-branch-elem),
	// so even 100M keys collapse in a handful of iterations.
	for len(pieces) > 1 {
		newRoot := &node{isLeaf: false, dirty: true, tx: tx, elements: pieces}
		var err error
		pieces, err = tx.spill(newRoot)
		if err != nil {
			return err
		}
	}
	if len(pieces) == 0 {
		tx.meta.root = 0
	} else {
		tx.meta.root = pieces[0].child
	}

	// Write the freelist to a fresh page.
	flBuf, flID, err := tx.allocPageBuf()
	if err != nil {
		return err
	}
	if err := tx.db.fl.writeTo(flBuf, flID); err != nil {
		return err
	}
	if err := tx.db.writePage(flID, flBuf); err != nil {
		return err
	}
	tx.meta.freelist = flID

	// Update high-water mark.
	if tx.db.highWater > tx.meta.pgid {
		tx.meta.pgid = tx.db.highWater
	}

	// Sync all data pages before we flip the meta page so a torn write
	// of the meta page can't point at half-written data.
	if err := tx.db.file.Sync(); err != nil {
		return err
	}

	// Write the alternate meta page.
	metaBuf := make([]byte, pageSize)
	tx.meta.writeTo(metaBuf)
	metaTarget := pgid(tx.meta.txid % 2)
	if err := tx.db.writePage(metaTarget, metaBuf); err != nil {
		return err
	}
	if err := tx.db.file.Sync(); err != nil {
		return err
	}

	// Successfully committed. Move pending old pages onto the freelist
	// under our txid (they'll be released when no older reader holds them).
	tx.db.meta = tx.meta
	tx.db.fl.release(tx.db.minReaderTxid())
	return nil
}

// Rollback discards any pending changes.
func (tx *Tx) Rollback() error {
	if tx.closed {
		return errTxClosed
	}
	tx.close()
	return nil
}

func (tx *Tx) close() {
	if tx.closed {
		return
	}
	tx.closed = true
	tx.db.endTx(tx)
}

// spill writes a dirty node (and its dirty descendants) to disk and
// returns the resulting (key, pgid) pieces that should replace the node
// in its parent. A single piece is the common case; multiple pieces
// happen when the node was too large for one page and had to split.
//
// For a branch node, spill first rebuilds the element list by replacing
// any dirty child entry with the pieces returned from that child's
// spill. This promotes splits "inline" rather than wrapping them in
// extra branch levels, which is what keeps the tree height proportional
// to log(N) rather than ballooning on every split.
func (tx *Tx) spill(n *node) ([]element, error) {
	if !n.dirty {
		// Unchanged node: keep its existing pgid and firstKey.
		return []element{{key: cloneBytes(n.firstKey()), child: n.pgid}}, nil
	}

	if !n.isLeaf {
		rebuilt := make([]element, 0, len(n.elements))
		for _, old := range n.elements {
			if child, ok := tx.nodes[old.child]; ok && child.dirty {
				childPieces, err := tx.spill(child)
				if err != nil {
					return nil, err
				}
				rebuilt = append(rebuilt, childPieces...)
			} else {
				rebuilt = append(rebuilt, old)
			}
		}
		n.elements = rebuilt
	}

	// Drop empty children produced by deletions. A branch that ends up
	// with zero elements collapses to nothing; the caller will fall back
	// to either a single child or an empty tree.
	if !n.isLeaf {
		filtered := n.elements[:0]
		for _, el := range n.elements {
			if el.child != 0 || el.key != nil {
				filtered = append(filtered, el)
			}
		}
		n.elements = filtered
	}

	if len(n.elements) == 0 {
		// Nothing to write. Free the old page if any.
		if n.pgid != 0 {
			tx.db.fl.markFree(tx.meta.txid, n.pgid)
		}
		return nil, nil
	}

	pieces := tx.split(n)
	result := make([]element, len(pieces))
	for i, piece := range pieces {
		id, err := tx.writeNode(piece)
		if err != nil {
			return nil, err
		}
		result[i] = element{key: cloneBytes(piece.firstKey()), child: id}
	}
	if n.pgid != 0 {
		tx.db.fl.markFree(tx.meta.txid, n.pgid)
	}
	return result, nil
}

// split divides n into the minimum number of page-sized pieces.
func (tx *Tx) split(n *node) []*node {
	if n.sizeOnDisk() <= pageSize {
		return []*node{n}
	}
	// Split roughly in half by element count, then re-check each piece.
	// We use a "fill ratio" approach: keep adding elements to the current
	// piece until adding one more would exceed pageSize, then start a new
	// piece.
	pieces := []*node{}
	current := &node{isLeaf: n.isLeaf, dirty: true, tx: tx}
	for _, el := range n.elements {
		test := &node{isLeaf: n.isLeaf, elements: append(current.elements, el)}
		if test.sizeOnDisk() > pageSize && len(current.elements) > 0 {
			pieces = append(pieces, current)
			current = &node{isLeaf: n.isLeaf, dirty: true, tx: tx}
		}
		current.elements = append(current.elements, el)
	}
	if len(current.elements) > 0 {
		pieces = append(pieces, current)
	}
	return pieces
}

// writeNode serializes a node onto a freshly allocated page.
func (tx *Tx) writeNode(n *node) (pgid, error) {
	buf, id, err := tx.allocPageBuf()
	if err != nil {
		return 0, err
	}
	if err := n.writeToPage(buf, id); err != nil {
		return 0, err
	}
	if err := tx.db.writePage(id, buf); err != nil {
		return 0, err
	}
	return id, nil
}

// allocPageBuf returns a zeroed 4KB buffer and a fresh page id.
func (tx *Tx) allocPageBuf() ([]byte, pgid, error) {
	id := tx.db.fl.allocate()
	if id == 0 {
		id = tx.db.highWater
		tx.db.highWater++
	}
	tx.pending = append(tx.pending, id)
	return make([]byte, pageSize), id, nil
}

// --- on-disk lookup helpers (no materialization) ---

func findInLeafPage(buf []byte, p *page, key []byte) []byte {
	count := int(p.count)
	// binary search by element key
	lo, hi := 0, count
	for lo < hi {
		mid := (lo + hi) / 2
		k := leafKeyAt(buf, mid)
		switch bytes.Compare(k, key) {
		case 0:
			return cloneBytes(leafValueAt(buf, mid))
		case -1:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return nil
}

func leafKeyAt(buf []byte, i int) []byte {
	off := pageHeaderSize + i*16
	ksz := uint32FromBuf(buf[off+0:])
	koff := uint32FromBuf(buf[off+8:])
	return buf[koff : koff+ksz]
}

func leafValueAt(buf []byte, i int) []byte {
	off := pageHeaderSize + i*16
	vsz := uint32FromBuf(buf[off+4:])
	voff := uint32FromBuf(buf[off+12:])
	return buf[voff : voff+vsz]
}

func branchChildForKey(buf []byte, p *page, key []byte) pgid {
	count := int(p.count)
	// Linear scan for now (branches are small).
	var lastChild pgid
	for i := 0; i < count; i++ {
		off := pageHeaderSize + i*16
		ksz := uint32FromBuf(buf[off+0:])
		koff := uint32FromBuf(buf[off+4:])
		ch := pgid(uint64FromBuf(buf[off+8:]))
		k := buf[koff : koff+ksz]
		if bytes.Compare(k, key) > 0 {
			if lastChild == 0 {
				return ch
			}
			return lastChild
		}
		lastChild = ch
	}
	return lastChild
}

func uint32FromBuf(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func uint64FromBuf(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}
