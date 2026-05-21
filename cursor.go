package littledb

import (
	"bytes"
)

// Cursor walks a snapshot of the database in key order. Cursors are
// always read-only; mutations go through Put/Delete on the transaction.
// A Cursor reads pages directly from disk via its parent transaction's
// captured meta, so writers running concurrently won't disturb it.
type Cursor struct {
	tx    *Tx
	stack []cursorFrame // path from root to current leaf
}

type cursorFrame struct {
	pgid pgid
	page *page
	buf  []byte
	idx  int
}

// Cursor returns a new cursor over the database snapshot held by this
// transaction.
func (tx *Tx) Cursor() *Cursor {
	return &Cursor{tx: tx}
}

// First positions the cursor at the smallest key. Returns nil, nil if
// the database is empty.
func (c *Cursor) First() (key, value []byte) {
	c.stack = c.stack[:0]
	if c.tx.meta.root == 0 {
		return nil, nil
	}
	c.descend(c.tx.meta.root, true)
	return c.kv()
}

// Last positions the cursor at the largest key.
func (c *Cursor) Last() (key, value []byte) {
	c.stack = c.stack[:0]
	if c.tx.meta.root == 0 {
		return nil, nil
	}
	c.descend(c.tx.meta.root, false)
	return c.kv()
}

// Seek positions the cursor at the first key >= target. If no such key
// exists the cursor is exhausted and Seek returns nil, nil.
func (c *Cursor) Seek(target []byte) (key, value []byte) {
	c.stack = c.stack[:0]
	if c.tx.meta.root == 0 {
		return nil, nil
	}
	c.descendSeek(c.tx.meta.root, target)
	k, v := c.kv()
	if k != nil && bytes.Compare(k, target) < 0 {
		return c.Next()
	}
	return k, v
}

// Next advances the cursor by one key.
func (c *Cursor) Next() (key, value []byte) {
	if len(c.stack) == 0 {
		return nil, nil
	}
	for {
		top := &c.stack[len(c.stack)-1]
		top.idx++
		if top.idx < int(top.page.count) {
			break
		}
		// past the end of this node; pop and continue from parent
		c.stack = c.stack[:len(c.stack)-1]
		if len(c.stack) == 0 {
			return nil, nil
		}
	}
	// Re-descend leftmost children if we landed on a branch.
	top := &c.stack[len(c.stack)-1]
	if top.page.flags&branchPage != 0 {
		off := pageHeaderSize + top.idx*16
		childID := pgid(uint64FromBuf(top.buf[off+8:]))
		c.descend(childID, true)
	}
	return c.kv()
}

// Prev steps the cursor back by one key. It's the mirror of Next:
// position with Last (or Seek) and call Prev to walk in descending
// order. Returns nil, nil once the cursor moves past the first key.
func (c *Cursor) Prev() (key, value []byte) {
	if len(c.stack) == 0 {
		return nil, nil
	}
	for {
		top := &c.stack[len(c.stack)-1]
		top.idx--
		if top.idx >= 0 {
			break
		}
		// before the start of this node; pop and continue from parent
		c.stack = c.stack[:len(c.stack)-1]
		if len(c.stack) == 0 {
			return nil, nil
		}
	}
	// Re-descend rightmost children if we landed on a branch.
	top := &c.stack[len(c.stack)-1]
	if top.page.flags&branchPage != 0 {
		off := pageHeaderSize + top.idx*16
		childID := pgid(uint64FromBuf(top.buf[off+8:]))
		c.descend(childID, false)
	}
	return c.kv()
}

// descend walks down to the leftmost (asc=true) or rightmost (asc=false)
// leaf starting at the given page, pushing frames onto the stack as it
// goes.
func (c *Cursor) descend(start pgid, asc bool) {
	id := start
	for {
		buf, err := c.tx.db.readPage(id)
		if err != nil {
			return
		}
		p := loadPage(buf)
		idx := 0
		if !asc {
			idx = int(p.count) - 1
		}
		c.stack = append(c.stack, cursorFrame{pgid: id, page: p, buf: buf, idx: idx})
		if p.flags&leafPage != 0 {
			return
		}
		off := pageHeaderSize + idx*16
		id = pgid(uint64FromBuf(buf[off+8:]))
	}
}

// descendSeek walks down to the leaf that should contain target,
// pushing branches' selected indices onto the stack.
func (c *Cursor) descendSeek(start pgid, target []byte) {
	id := start
	for {
		buf, err := c.tx.db.readPage(id)
		if err != nil {
			return
		}
		p := loadPage(buf)
		if p.flags&leafPage != 0 {
			// binary search for first key >= target
			lo, hi := 0, int(p.count)
			for lo < hi {
				mid := (lo + hi) / 2
				if bytes.Compare(leafKeyAt(buf, mid), target) < 0 {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			c.stack = append(c.stack, cursorFrame{pgid: id, page: p, buf: buf, idx: lo})
			return
		}
		// Branch: pick the child that would hold target.
		count := int(p.count)
		idx := 0
		for i := 0; i < count; i++ {
			off := pageHeaderSize + i*16
			ksz := uint32FromBuf(buf[off+0:])
			koff := uint32FromBuf(buf[off+4:])
			k := buf[koff : koff+ksz]
			if bytes.Compare(k, target) > 0 {
				break
			}
			idx = i
		}
		c.stack = append(c.stack, cursorFrame{pgid: id, page: p, buf: buf, idx: idx})
		off := pageHeaderSize + idx*16
		id = pgid(uint64FromBuf(buf[off+8:]))
	}
}

// kv returns the (key, value) at the cursor's current leaf position.
// Returns nil, nil if the cursor isn't positioned on a leaf element.
func (c *Cursor) kv() (key, value []byte) {
	if len(c.stack) == 0 {
		return nil, nil
	}
	top := c.stack[len(c.stack)-1]
	if top.page.flags&leafPage == 0 {
		return nil, nil
	}
	if top.idx >= int(top.page.count) {
		return nil, nil
	}
	return cloneBytes(leafKeyAt(top.buf, top.idx)), cloneBytes(leafValueAt(top.buf, top.idx))
}

// Range invokes fn for every key in [start, end). A nil start means
// "from the beginning"; a nil end means "until the last key". fn may
// return a non-nil error to stop iteration early; that error is
// returned to the caller.
func (tx *Tx) Range(start, end []byte, fn func(key, value []byte) error) error {
	c := tx.Cursor()
	var k, v []byte
	if start == nil {
		k, v = c.First()
	} else {
		k, v = c.Seek(start)
	}
	for k != nil {
		if end != nil && bytes.Compare(k, end) >= 0 {
			return nil
		}
		if err := fn(k, v); err != nil {
			return err
		}
		k, v = c.Next()
	}
	return nil
}
