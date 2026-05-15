package littledb_test

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/c-tonneslan/littledb"
	bolt "go.etcd.io/bbolt"
)

// These benchmarks compare littledb against bbolt (the maintained fork
// of BoltDB) on a few common workloads. Both use single-file COW B+trees
// with two-meta-page commits, so the apples-to-apples comparison is fair.
// bbolt is mmap-based and battle-tested; littledb is regular file I/O
// and a few hundred lines. Expect bbolt to win the steady-state reads,
// littledb to be in the same ballpark on bulk writes.

func benchKey(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func benchValue(size int) []byte {
	v := make([]byte, size)
	rand.Read(v)
	return v
}

func BenchmarkLittleDBPut(b *testing.B) {
	dir := b.TempDir()
	db, err := littledb.Open(filepath.Join(dir, "lt.db"), 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *littledb.Tx) error {
			return tx.Put(benchKey(i), val)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltPut(b *testing.B) {
	dir := b.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("b"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket([]byte("b")).Put(benchKey(i), val)
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLittleDBPutBatch(b *testing.B) {
	dir := b.TempDir()
	db, err := littledb.Open(filepath.Join(dir, "lt.db"), 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)

	const batch = 1000
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		end := i + batch
		if end > b.N {
			end = b.N
		}
		if err := db.Update(func(tx *littledb.Tx) error {
			for j := i; j < end; j++ {
				if err := tx.Put(benchKey(j), val); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltPutBatch(b *testing.B) {
	dir := b.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("b"))
		return err
	}); err != nil {
		b.Fatal(err)
	}

	const batch = 1000
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		end := i + batch
		if end > b.N {
			end = b.N
		}
		if err := db.Update(func(tx *bolt.Tx) error {
			bk := tx.Bucket([]byte("b"))
			for j := i; j < end; j++ {
				if err := bk.Put(benchKey(j), val); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLittleDBGet(b *testing.B) {
	dir := b.TempDir()
	db, err := littledb.Open(filepath.Join(dir, "lt.db"), 0600)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)

	const n = 100000
	if err := db.Update(func(tx *littledb.Tx) error {
		for i := 0; i < n; i++ {
			if err := tx.Put(benchKey(i), val); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *littledb.Tx) error {
			tx.Get(benchKey(i % n))
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBoltGet(b *testing.B) {
	dir := b.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "bolt.db"), 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	val := benchValue(64)

	const n = 100000
	if err := db.Update(func(tx *bolt.Tx) error {
		bk, err := tx.CreateBucketIfNotExists([]byte("b"))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := bk.Put(benchKey(i), val); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.View(func(tx *bolt.Tx) error {
			tx.Bucket([]byte("b")).Get(benchKey(i % n))
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// Reports the on-disk file size after a fixed write workload. Lower is
// better. COW databases pay an amplification cost when small commits
// each rewrite a tree path.
func BenchmarkLittleDBFileSize(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "lt.db")
			db, err := littledb.Open(path, 0600)
			if err != nil {
				b.Fatal(err)
			}
			val := benchValue(64)
			for i := 0; i < n; i++ {
				if err := db.Update(func(tx *littledb.Tx) error {
					return tx.Put(benchKey(i), val)
				}); err != nil {
					b.Fatal(err)
				}
			}
			db.Close()
			st, _ := os.Stat(path)
			b.ReportMetric(float64(st.Size())/1024/1024, "MB")
			b.ReportMetric(float64(st.Size())/float64(n), "bytes/key")
		})
	}
}
