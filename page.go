package littledb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"unsafe"
)

// pgid is a page identifier. Page 0 and 1 are the two meta pages; everything
// after that is allocated dynamically.
type pgid uint64

const (
	pageSize     = 4096
	metaPgidA    pgid = 0
	metaPgidB    pgid = 1
	maxKeySize   = 1 << 15 // 32KB
	maxValueSize = 1 << 20 // 1MB
)

// pageFlags identify what's in a page.
const (
	metaPage     uint16 = 0x01
	leafPage     uint16 = 0x02
	branchPage   uint16 = 0x04
	freelistPage uint16 = 0x08
)

// page is the common header at the start of every on-disk page.
// Layout matches what we write to disk; we use unsafe casts elsewhere
// to read pages without copying.
type page struct {
	id    pgid
	flags uint16
	count uint16 // number of elements (or for freelist, count of pgids)
	_     uint32 // padding to 8-byte align the next field
}

const pageHeaderSize = int(unsafe.Sizeof(page{}))

// loadPage interprets the first bytes of buf as a page header.
func loadPage(buf []byte) *page {
	return (*page)(unsafe.Pointer(&buf[0]))
}

// meta is the contents of a meta page. We keep two copies on disk
// (page 0 and page 1) and write alternating copies so a crash during
// a commit always leaves at least one valid meta page on disk.
type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32 // reserved
	root     pgid   // root of the B+tree
	freelist pgid   // page id where freelist is serialized
	pgid     pgid   // high-water mark (next page to allocate)
	txid     uint64 // transaction id, monotonically increasing
	checksum uint64 // crc32c of all bytes above, zero-extended
}

const (
	// magicNumber is the four-byte signature at the start of every meta
	// page: 'L', 'D', 'B', 0x01.
	magicNumber   uint32 = 0x014244_4c // little-endian "LDB\x01"
	formatVersion uint32 = 1
)

var (
	errInvalidMagic    = errors.New("littledb: file is not a littledb database")
	errVersionMismatch = errors.New("littledb: file format version mismatch")
	errBothMetaCorrupt = errors.New("littledb: both meta pages are corrupt")
)

// writeTo serializes the meta page (with checksum) into the provided buffer,
// which must be at least pageSize bytes long. Buffer is zeroed first.
func (m *meta) writeTo(buf []byte) {
	for i := range buf[:pageSize] {
		buf[i] = 0
	}
	// page header
	hdr := loadPage(buf)
	hdr.flags = metaPage
	// the meta contents go right after the header
	body := buf[pageHeaderSize:]
	binary.LittleEndian.PutUint32(body[0:], m.magic)
	binary.LittleEndian.PutUint32(body[4:], m.version)
	binary.LittleEndian.PutUint32(body[8:], m.pageSize)
	binary.LittleEndian.PutUint32(body[12:], m.flags)
	binary.LittleEndian.PutUint64(body[16:], uint64(m.root))
	binary.LittleEndian.PutUint64(body[24:], uint64(m.freelist))
	binary.LittleEndian.PutUint64(body[32:], uint64(m.pgid))
	binary.LittleEndian.PutUint64(body[40:], m.txid)
	// checksum covers everything we just wrote (header + body fields)
	end := pageHeaderSize + 48
	sum := crc32.Checksum(buf[:end], crc32cTable)
	binary.LittleEndian.PutUint64(body[48:], uint64(sum))
}

// readMeta parses a meta page from buf, returning errInvalidMagic if the
// magic doesn't match, errVersionMismatch on a version mismatch, or
// nil meta if the checksum doesn't validate.
func readMeta(buf []byte) (*meta, error) {
	hdr := loadPage(buf)
	if hdr.flags&metaPage == 0 {
		return nil, errInvalidMagic
	}
	body := buf[pageHeaderSize:]
	m := &meta{
		magic:    binary.LittleEndian.Uint32(body[0:]),
		version:  binary.LittleEndian.Uint32(body[4:]),
		pageSize: binary.LittleEndian.Uint32(body[8:]),
		flags:    binary.LittleEndian.Uint32(body[12:]),
		root:     pgid(binary.LittleEndian.Uint64(body[16:])),
		freelist: pgid(binary.LittleEndian.Uint64(body[24:])),
		pgid:     pgid(binary.LittleEndian.Uint64(body[32:])),
		txid:     binary.LittleEndian.Uint64(body[40:]),
		checksum: binary.LittleEndian.Uint64(body[48:]),
	}
	if m.magic != magicNumber {
		return nil, errInvalidMagic
	}
	if m.version != formatVersion {
		return nil, errVersionMismatch
	}
	// Recompute checksum with the stored checksum zeroed.
	saved := make([]byte, 8)
	copy(saved, body[48:56])
	for i := 48; i < 56; i++ {
		body[i] = 0
	}
	end := pageHeaderSize + 48
	want := uint64(crc32.Checksum(buf[:end], crc32cTable))
	copy(body[48:56], saved)
	if want != m.checksum {
		return nil, nil // signal "corrupt" without erroring
	}
	return m, nil
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
