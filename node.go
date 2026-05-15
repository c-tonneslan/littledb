package littledb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// A node is the in-memory representation of a B+tree page. We materialize
// pages into nodes when a transaction needs to mutate them; the page on
// disk is immutable until commit decides which pages survive.
type node struct {
	pgid     pgid      // page id this node was loaded from (0 if newly allocated)
	parent   *node     // parent in the tree, or nil for the root
	isLeaf   bool      // true for leaves, false for internal branch nodes
	elements []element // sorted by key
	tx       *Tx       // the transaction this node belongs to
	dirty    bool      // true if the node has been mutated
}

// An element holds one key in a node. For leaves, value holds the user
// value. For branch nodes, child holds the page id of the subtree whose
// keys are all >= key.
type element struct {
	key   []byte
	value []byte // leaf only
	child pgid   // branch only
}

// minKeysPerNode prevents nodes from getting too sparse. Real B-trees set
// this to ceil(branchingFactor / 2) but for a flat 4KB page with variable
// sized keys we just use a conservative byte threshold instead (see
// needsRebalance below).

// sizeOnDisk returns how many bytes the node would consume if written
// as a page right now: header + offset array + packed key/value data.
func (n *node) sizeOnDisk() int {
	sz := pageHeaderSize
	for _, el := range n.elements {
		sz += 16 // element header (see writeToPage layout)
		sz += len(el.key)
		if n.isLeaf {
			sz += len(el.value)
		}
	}
	return sz
}

// search returns the index of the first element with key >= target.
// If exact is true the target was found at the returned index.
func (n *node) search(target []byte) (idx int, exact bool) {
	idx = sort.Search(len(n.elements), func(i int) bool {
		return bytes.Compare(n.elements[i].key, target) >= 0
	})
	if idx < len(n.elements) && bytes.Equal(n.elements[idx].key, target) {
		return idx, true
	}
	return idx, false
}

// put inserts or replaces a key/value pair (leaf) or a key/child pair (branch).
// Caller must mark the node dirty.
func (n *node) put(key, value []byte, child pgid) {
	idx, exact := n.search(key)
	if exact {
		n.elements[idx].value = value
		if !n.isLeaf {
			n.elements[idx].child = child
		}
		return
	}
	// Insert at idx.
	n.elements = append(n.elements, element{})
	copy(n.elements[idx+1:], n.elements[idx:])
	n.elements[idx] = element{key: cloneBytes(key), value: cloneBytes(value), child: child}
}

// del removes the element with the given key. Returns true if removed.
func (n *node) del(key []byte) bool {
	idx, exact := n.search(key)
	if !exact {
		return false
	}
	n.elements = append(n.elements[:idx], n.elements[idx+1:]...)
	return true
}

// loadNode deserializes a disk page into an in-memory node.
func loadNode(p *page, raw []byte) *node {
	n := &node{
		pgid:   p.id,
		isLeaf: p.flags&leafPage != 0,
	}
	n.elements = make([]element, int(p.count))
	off := pageHeaderSize
	for i := 0; i < int(p.count); i++ {
		hdr := raw[off : off+16]
		ksz := binary.LittleEndian.Uint32(hdr[0:])
		if n.isLeaf {
			vsz := binary.LittleEndian.Uint32(hdr[4:])
			koff := binary.LittleEndian.Uint32(hdr[8:])
			voff := binary.LittleEndian.Uint32(hdr[12:])
			n.elements[i] = element{
				key:   cloneBytes(raw[koff : koff+ksz]),
				value: cloneBytes(raw[voff : voff+vsz]),
			}
		} else {
			koff := binary.LittleEndian.Uint32(hdr[4:])
			ch := binary.LittleEndian.Uint64(hdr[8:])
			n.elements[i] = element{
				key:   cloneBytes(raw[koff : koff+ksz]),
				child: pgid(ch),
			}
		}
		off += 16
	}
	return n
}

// writeToPage serializes the node into a fresh page buffer. dataEnd is
// the absolute offset within buf where packed key/value bytes start
// growing backward. The function returns the number of bytes that don't
// fit if the node is too large to write, otherwise 0.
func (n *node) writeToPage(buf []byte, id pgid) error {
	if n.sizeOnDisk() > len(buf) {
		return fmt.Errorf("littledb: node too large for one page (%d bytes)", n.sizeOnDisk())
	}
	// zero the page
	for i := range buf {
		buf[i] = 0
	}
	hdr := loadPage(buf)
	hdr.id = id
	if n.isLeaf {
		hdr.flags = leafPage
	} else {
		hdr.flags = branchPage
	}
	hdr.count = uint16(len(n.elements))

	// Elements are written from the start; key/value bytes grow backward
	// from the end of the page. This keeps the variable-length data
	// densely packed and makes binary search by index trivial.
	dataEnd := uint32(len(buf))
	elOff := pageHeaderSize
	for _, el := range n.elements {
		ksz := uint32(len(el.key))
		dataEnd -= ksz
		copy(buf[dataEnd:], el.key)
		koff := dataEnd

		if n.isLeaf {
			vsz := uint32(len(el.value))
			dataEnd -= vsz
			copy(buf[dataEnd:], el.value)
			voff := dataEnd
			binary.LittleEndian.PutUint32(buf[elOff+0:], ksz)
			binary.LittleEndian.PutUint32(buf[elOff+4:], vsz)
			binary.LittleEndian.PutUint32(buf[elOff+8:], koff)
			binary.LittleEndian.PutUint32(buf[elOff+12:], voff)
		} else {
			binary.LittleEndian.PutUint32(buf[elOff+0:], ksz)
			binary.LittleEndian.PutUint32(buf[elOff+4:], koff)
			binary.LittleEndian.PutUint64(buf[elOff+8:], uint64(el.child))
		}
		elOff += 16
		if uint32(elOff) > dataEnd {
			return fmt.Errorf("littledb: node layout overflow during write")
		}
	}
	return nil
}

// firstKey returns the smallest key in the node, used as the separator
// when promoting a split node into its parent.
func (n *node) firstKey() []byte {
	if len(n.elements) == 0 {
		return nil
	}
	return n.elements[0].key
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
