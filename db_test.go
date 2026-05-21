package littledb

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDB(t *testing.T) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, 0600)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db, path
}

func TestOpenEmpty(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()

	if err := db.View(func(tx *Tx) error {
		if got := tx.Get([]byte("missing")); got != nil {
			return fmt.Errorf("expected nil, got %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPutGetSingle(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		return tx.Put([]byte("hello"), []byte("world"))
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		got := tx.Get([]byte("hello"))
		if !bytes.Equal(got, []byte("world")) {
			return fmt.Errorf("got %q want %q", got, "world")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPutOverwrite(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	for _, v := range []string{"v1", "v2", "v3"} {
		if err := db.Update(func(tx *Tx) error {
			return tx.Put([]byte("k"), []byte(v))
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.View(func(tx *Tx) error {
		got := tx.Get([]byte("k"))
		if !bytes.Equal(got, []byte("v3")) {
			return fmt.Errorf("got %q want v3", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPutMany(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()

	const n = 5000
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			v := []byte(fmt.Sprintf("value-%d-padding-to-be-bigger-than-the-key", i))
			if err := tx.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		for i := 0; i < n; i++ {
			k := []byte(fmt.Sprintf("key-%08d", i))
			want := []byte(fmt.Sprintf("value-%d-padding-to-be-bigger-than-the-key", i))
			got := tx.Get(k)
			if !bytes.Equal(got, want) {
				return fmt.Errorf("key %d: got %q want %q", i, got, want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("x")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i += 2 {
			if err := tx.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			got := tx.Get([]byte(fmt.Sprintf("k%03d", i)))
			if i%2 == 0 {
				if got != nil {
					return fmt.Errorf("expected k%d to be deleted, got %q", i, got)
				}
			} else {
				if !bytes.Equal(got, []byte("x")) {
					return fmt.Errorf("expected k%d to be 'x', got %q", i, got)
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")
	db, err := Open(path, 0600)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		v := []byte(fmt.Sprintf("v%04d", i))
		if err := db.Update(func(tx *Tx) error { return tx.Put(k, v) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(path, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		want := []byte(fmt.Sprintf("v%04d", i))
		var got []byte
		if err := db2.View(func(tx *Tx) error {
			got = tx.Get(k)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}
}

func TestConcurrentReaders(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = db.View(func(tx *Tx) error {
					tx.Get([]byte(fmt.Sprintf("k%04d", i)))
					return nil
				})
			}
		}()
	}
	wg.Wait()
}

func TestRandomOps(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()

	rng := rand.New(rand.NewSource(42))
	truth := map[string]string{}

	for op := 0; op < 2000; op++ {
		k := fmt.Sprintf("k%05d", rng.Intn(500))
		if rng.Intn(3) == 0 {
			// delete
			if err := db.Update(func(tx *Tx) error { return tx.Delete([]byte(k)) }); err != nil {
				t.Fatal(err)
			}
			delete(truth, k)
		} else {
			v := fmt.Sprintf("v%d", rng.Intn(1<<16))
			if err := db.Update(func(tx *Tx) error { return tx.Put([]byte(k), []byte(v)) }); err != nil {
				t.Fatal(err)
			}
			truth[k] = v
		}
	}

	if err := db.View(func(tx *Tx) error {
		for k, v := range truth {
			got := tx.Get([]byte(k))
			if !bytes.Equal(got, []byte(v)) {
				return fmt.Errorf("key %s: got %q want %q", k, got, v)
			}
		}
		// Spot check a few keys that should be absent.
		for i := 0; i < 500; i++ {
			k := fmt.Sprintf("k%05d", i)
			if _, alive := truth[k]; alive {
				continue
			}
			if got := tx.Get([]byte(k)); got != nil {
				return fmt.Errorf("key %s should be absent, got %q", k, got)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRollback(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()

	if err := db.Update(func(tx *Tx) error { return tx.Put([]byte("k"), []byte("v1")) }); err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		if got := tx.Get([]byte("k")); !bytes.Equal(got, []byte("v1")) {
			return fmt.Errorf("rollback failed: got %q want v1", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverFromCorruptMeta(t *testing.T) {
	// Write two commits, then corrupt one of the meta pages. The other
	// should always hold at least the next-most-recent state, so we
	// should be able to recover the first key regardless of which meta
	// got corrupted.
	for _, victim := range []pgid{metaPgidA, metaPgidB} {
		t.Run(fmt.Sprintf("meta-%d", victim), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "corrupt.db")
			db, err := Open(path, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				return tx.Put([]byte("first"), []byte("1"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				return tx.Put([]byte("second"), []byte("2"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			f, err := os.OpenFile(path, os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			zero := make([]byte, pageSize)
			if _, err := f.WriteAt(zero, int64(victim)*int64(pageSize)); err != nil {
				t.Fatal(err)
			}
			f.Close()

			db2, err := Open(path, 0600)
			if err != nil {
				t.Fatalf("expected recovery, got: %v", err)
			}
			defer db2.Close()

			// The surviving meta page holds at minimum the older commit
			// (or both if we corrupted the older one). Either way the
			// first key must be present.
			if err := db2.View(func(tx *Tx) error {
				got := tx.Get([]byte("first"))
				if !bytes.Equal(got, []byte("1")) {
					return fmt.Errorf("got %q want '1'", got)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRangeFullScan(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		var seen []string
		err := tx.Range(nil, nil, func(k, v []byte) error {
			seen = append(seen, string(k))
			return nil
		})
		if err != nil {
			return err
		}
		if len(seen) != 1000 {
			return fmt.Errorf("expected 1000 keys, got %d", len(seen))
		}
		for i := 0; i < 1000; i++ {
			want := fmt.Sprintf("k%04d", i)
			if seen[i] != want {
				return fmt.Errorf("idx %d: got %q want %q", i, seen[i], want)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCursorPrevWalksBackward(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		c := tx.Cursor()
		var seen []string
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			seen = append(seen, string(k))
		}
		if len(seen) != 1000 {
			return fmt.Errorf("expected 1000 keys, got %d", len(seen))
		}
		for i := 0; i < 1000; i++ {
			want := fmt.Sprintf("k%04d", 999-i)
			if seen[i] != want {
				return fmt.Errorf("idx %d: got %q want %q", i, seen[i], want)
			}
		}
		// Prev past the first key is exhausted, not a wraparound.
		if k, _ := c.Prev(); k != nil {
			return fmt.Errorf("Prev past the start returned %q, want nil", k)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCursorPrevFromSeek(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("x")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		c := tx.Cursor()
		if k, _ := c.Seek([]byte("k050")); string(k) != "k050" {
			return fmt.Errorf("Seek landed on %q, want k050", k)
		}
		if k, _ := c.Prev(); string(k) != "k049" {
			return fmt.Errorf("Prev gave %q, want k049", k)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRangeBounded(t *testing.T) {
	db, _ := tempDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			if err := tx.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("x")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		var seen []string
		err := tx.Range([]byte("k010"), []byte("k020"), func(k, v []byte) error {
			seen = append(seen, string(k))
			return nil
		})
		if err != nil {
			return err
		}
		if len(seen) != 10 {
			return fmt.Errorf("expected 10 keys in [k010,k020), got %d: %v", len(seen), seen)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
