# littledb

A tiny embedded key/value store in Go. Single file on disk, ACID transactions, copy-on-write B+tree. Around 1,500 lines of code that exists mostly to teach itself how LMDB and BoltDB work.

```go
db, err := littledb.Open("data.db", 0600)
if err != nil { ... }
defer db.Close()

// write
err = db.Update(func(tx *littledb.Tx) error {
    return tx.Put([]byte("hello"), []byte("world"))
})

// read
err = db.View(func(tx *littledb.Tx) error {
    fmt.Println(string(tx.Get([]byte("hello"))))
    return nil
})

// range scan
err = db.View(func(tx *littledb.Tx) error {
    return tx.Range([]byte("a"), []byte("z"), func(k, v []byte) error {
        fmt.Printf("%s=%s\n", k, v)
        return nil
    })
})
```

## Why

I wanted to actually understand how COW B+tree databases work, not just use one. There are exactly two interesting moves in a database like this and I'd never implemented either:

1. The "swap atomically by writing one of two meta pages last" trick. It's how LMDB and BoltDB guarantee crash safety without a separate write-ahead log.
2. The "free old pages when no reader can still see them" trick. It's how you do MVCC without a vacuum step.

littledb does both. The whole thing is about a thousand lines of code split across six files.

## How it works

### Pages

Everything is a 4KB page. Page 0 and page 1 are the two meta pages. Pages 2 onward hold the tree, the freelist, and the user's data.

Every meta page has the same fields: magic number, format version, page size, root tree pgid, freelist pgid, high-water mark, txid, and a CRC32C checksum that covers all of it.

### Copy-on-write

Writes never overwrite live pages. When a transaction modifies a leaf, we allocate a fresh page for the new version, write it there, and only at commit time do we publish the new tree by overwriting one of the two meta pages.

Concretely, commit does:

1. Walk the dirty subtree bottom-up. For each dirty node, write a fresh page; record the new pgid.
2. Bubble new pgids up to the root, rebuilding parents inline so split pieces become siblings rather than wrapped in extra branch levels.
3. Write the freelist to a fresh page.
4. `fsync` the data.
5. Write the new meta page. The txid alternates which physical page (0 or 1) we hit, so the previous committed state always survives on the other page.
6. `fsync` the meta.

If we crash anywhere in steps 1 through 5, the other meta page still points at the previous committed tree. The orphaned new pages are unreachable; we don't know they were ever written. On the next open we read both meta pages, pick the one with the higher valid checksum and txid, and continue from there.

### MVCC reads

A read transaction snapshots the meta at `Begin` time and never observes mutations from later writers. Because writers never touch a live page, readers walking the tree from their snapshot meta see a consistent, unchanging view.

Old pages can't be reused until every reader who might be looking at them has finished. The freelist tracks "pending" pages per txid; on each commit we ask "what's the smallest txid still in use by any open reader?" and release everything older than that. No vacuum, no compaction, just bookkeeping.

### B+tree

Leaf nodes hold sorted (key, value) elements. Branch nodes hold sorted (key, child-pgid) elements where the child holds keys `>= key`. Element headers are 16 bytes and live at the start of the page; the actual key and value bytes grow backward from the end of the page so we can binary-search on element offsets without touching the data.

On insert, we walk the tree, mutate the leaf in memory, and let commit's spill step decide where to split. On delete we do the same, then drop empty branches at spill time. There's no merge or rebalance step yet, which means a workload of "insert many, then delete most" leaves a sparser tree than it could be. Adding rebalance is on the list.

### What's intentionally not here

- **No mmap.** Reads go through `ReadAt`. This is slower than mmap-based stores (see benchmarks) but the code is simpler, portable, and easier to reason about. It also means littledb doesn't have to deal with mmap-related crashes when a goroutine gets killed mid-read.
- **No buckets / nested namespaces.** There's one flat keyspace. If you want buckets, prefix your keys with a bucket name.
- **No multiple concurrent writers.** One writable transaction at a time; readers don't block writers.

## Benchmarks vs bbolt

`go test -bench .` against [bbolt](https://github.com/etcd-io/bbolt) (the maintained fork of BoltDB). 64-byte values, 8-byte keys.

```
BenchmarkLittleDBPut-14         8.6 ms/op    # one Put per transaction with fsync
BenchmarkBoltPut-14             8.3 ms/op    # ditto

BenchmarkLittleDBPutBatch-14    8.4 µs/op    # 1000 Puts per transaction
BenchmarkBoltPutBatch-14        8.4 µs/op

BenchmarkLittleDBGet-14         2.5 µs/op    # point lookup on 100k keys
BenchmarkBoltGet-14             0.4 µs/op
```

Writes are basically tied. Both databases are bottlenecked on `fsync` at the meta-page swap, and a small Go implementation can match bbolt there.

Reads are about 6x slower. bbolt uses mmap so lookups never copy a page; littledb does a 4KB `ReadAt` per branch/leaf traversal. That's the price of "no mmap" and I'm fine with it for a learning project.

File size is comparable too: bulk loading 10,000 64-byte values produces a ~40MB file in both, since both use 4KB pages with similar fanout.

## Crash recovery

There's a test (`TestRecoverFromCorruptMeta`) that opens a database, writes two commits, zeroes one of the two meta pages, and verifies the database still opens with the surviving meta. It runs against both meta pages as the victim.

The on-disk format also guarantees that a partial write of either meta page can't be mistaken for a valid one: the CRC32C checksum lives inside the meta and covers all the other fields.

## Limitations / future work

- No B+tree merge/rebalance on delete. A high-churn workload eventually has under-full nodes.
- The freelist serializes to a single page, capping it at a few hundred entries. A database that frees a huge number of pages in one transaction would error out. Easy to fix with overflow pages, just haven't yet.
- No multi-writer support, no MVCC for write/write conflicts. Use a different database if you need that.

## Acknowledgements

The on-disk format and commit algorithm are deliberately close to [BoltDB](https://github.com/boltdb/bolt) and through it to [LMDB](http://www.lmdb.tech/doc/). The point of this code wasn't to be original, it was to actually understand those designs by writing one. Read [Howard Chu's LMDB paper](https://www.openldap.org/pub/hyc/mdb-paper.pdf) and the [BoltDB README](https://github.com/etcd-io/bbolt#readme) if you want the canonical version.

MIT. Built by [Charlie Tonneslan](https://c-tonneslan-portfolio.vercel.app/).
