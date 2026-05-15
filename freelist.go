package littledb

import (
	"encoding/binary"
	"sort"
)

// freelist tracks page ids that are free for reuse. Because we use
// copy-on-write, a page that gets overwritten in a transaction can't be
// freed immediately, the old version is still the live tree until the
// commit finishes. We instead stash the freed pages under the txid of
// the transaction that retired them, and release them once no open
// reader could still be looking at that snapshot.
type freelist struct {
	free    []pgid          // page ids available right now
	pending map[uint64][]pgid // txid -> pages freed by that txn (not yet released)
}

func newFreelist() *freelist {
	return &freelist{pending: make(map[uint64][]pgid)}
}

// allocate returns a free page id or 0 if none is available.
func (f *freelist) allocate() pgid {
	if len(f.free) == 0 {
		return 0
	}
	id := f.free[0]
	f.free = f.free[1:]
	return id
}

// markFree records that the given page id was freed by the given
// transaction. The page won't be reused until release determines it's
// no longer reachable by any open reader.
func (f *freelist) markFree(txid uint64, id pgid) {
	f.pending[txid] = append(f.pending[txid], id)
}

// release moves pages freed at or before minTxid back into the free pool.
// minTxid is the lowest txid still in use by any open reader; pages
// freed by older transactions are guaranteed unreachable.
func (f *freelist) release(minTxid uint64) {
	for txid, pages := range f.pending {
		if txid <= minTxid {
			f.free = append(f.free, pages...)
			delete(f.pending, txid)
		}
	}
	sort.Slice(f.free, func(i, j int) bool { return f.free[i] < f.free[j] })
}

// count returns the number of free pages right now (excluding pending).
func (f *freelist) count() int { return len(f.free) }

// pendingCount returns the total number of pending (not-yet-released) pages.
func (f *freelist) pendingCount() int {
	n := 0
	for _, p := range f.pending {
		n += len(p)
	}
	return n
}

// pageSize returns how many bytes the freelist needs on disk. The
// freelist serializes as a single page (we cap the number of free
// entries that fit) — if we ever overflow we'd need overflow pages, but
// for v0.1 we'll just fail loudly.
func (f *freelist) pageBytes() int {
	return pageHeaderSize + 8 + 8*len(f.free)
}

// writeTo serializes the freelist into the provided page buffer.
// Pending entries are intentionally NOT written; they're tracked in
// memory only. On open we read them back as free.
func (f *freelist) writeTo(buf []byte, id pgid) error {
	if f.pageBytes() > len(buf) {
		return errFreelistTooLarge
	}
	for i := range buf {
		buf[i] = 0
	}
	hdr := loadPage(buf)
	hdr.id = id
	hdr.flags = freelistPage
	hdr.count = 0 // we use the explicit length field below instead
	binary.LittleEndian.PutUint64(buf[pageHeaderSize:], uint64(len(f.free)))
	off := pageHeaderSize + 8
	for _, id := range f.free {
		binary.LittleEndian.PutUint64(buf[off:], uint64(id))
		off += 8
	}
	return nil
}

// readFrom deserializes a freelist page.
func (f *freelist) readFrom(buf []byte) {
	n := binary.LittleEndian.Uint64(buf[pageHeaderSize:])
	f.free = make([]pgid, n)
	off := pageHeaderSize + 8
	for i := range f.free {
		f.free[i] = pgid(binary.LittleEndian.Uint64(buf[off:]))
		off += 8
	}
}
