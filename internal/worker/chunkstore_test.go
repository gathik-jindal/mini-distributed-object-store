package worker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
)

func newStore(t *testing.T) *ChunkStore {
	t.Helper()
	s, err := NewChunkStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func put(t *testing.T, s *ChunkStore, id string, data []byte) bool {
	t.Helper()
	created, err := s.Put(id, data, checksum.SHA256Hex(data), "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	return created
}

func corrupt(t *testing.T, s *ChunkStore, id string) {
	t.Helper()
	p := s.DataPath(id)
	b, _ := os.ReadFile(p)
	b[0] ^= 0xff
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	s := newStore(t)
	data := []byte("hello world, this is a chunk")
	if !put(t, s, "obj-c00000", data) {
		t.Fatal("expected created")
	}
	got, meta, err := s.Get("obj-c00000")
	if err != nil || string(got) != string(data) {
		t.Fatalf("get: %v %q", err, got)
	}
	if meta.Checksum != checksum.SHA256Hex(data) || meta.Size != int64(len(data)) || meta.ObjectID != "obj" {
		t.Fatalf("bad meta %+v", meta)
	}
}

func TestPutRejectsBadChecksum(t *testing.T) {
	s := newStore(t)
	_, err := s.Put("x-c00000", []byte("abc"), "0000", "", nil)
	var mm *ChecksumMismatchError
	if !errors.As(err, &mm) {
		t.Fatalf("want mismatch, got %v", err)
	}
	if _, err := os.Stat(s.DataPath("x-c00000")); !os.IsNotExist(err) {
		t.Fatal("rejected chunk must not be stored")
	}
}

func TestPutIdempotentAndConflict(t *testing.T) {
	s := newStore(t)
	if !put(t, s, "x-c00000", []byte("abc")) || put(t, s, "x-c00000", []byte("abc")) {
		t.Fatal("first put creates, second is a no-op")
	}
	if _, err := s.Put("x-c00000", []byte("xyz"), checksum.SHA256Hex([]byte("xyz")), "", nil); !errors.Is(err, ErrChunkConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestScrubAndRepairOverwrite(t *testing.T) {
	s := newStore(t)
	data := []byte("some chunk bytes that will be damaged")
	put(t, s, "x-c00000", data)
	corrupt(t, s, "x-c00000")
	scanned, bad, err := s.Scrub()
	if err != nil || scanned != 1 || len(bad) != 1 || len(s.CorruptChunks()) != 1 {
		t.Fatalf("scrub: %d %v %v", scanned, bad, err)
	}
	if !put(t, s, "x-c00000", data) {
		t.Fatal("rewriting a corrupt copy must overwrite it")
	}
	if got, _, _ := s.Get("x-c00000"); string(got) != string(data) {
		t.Fatal("not repaired")
	}
	if len(s.CorruptChunks()) != 0 {
		t.Fatal("corrupt set not cleared")
	}
	if _, bad, _ := s.Scrub(); len(bad) != 0 {
		t.Fatal("still corrupt")
	}
}

func TestDeleteAndMissing(t *testing.T) {
	s := newStore(t)
	put(t, s, "x-c00000", []byte("q"))
	if found, _ := s.Delete("x-c00000"); !found {
		t.Fatal("expected delete")
	}
	if found, _ := s.Delete("x-c00000"); found {
		t.Fatal("second delete finds nothing")
	}
	if _, _, err := s.Get("x-c00000"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
}

func TestListAndStats(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 5; i++ {
		put(t, s, "o-c0000"+string(rune('0'+i)), make([]byte, i+1))
	}
	entries, err := s.List(true)
	if err != nil || len(entries) != 5 {
		t.Fatalf("list: %v %d", err, len(entries))
	}
	for _, e := range entries {
		if e.Corrupt {
			t.Fatal("nothing should be corrupt")
		}
	}
	used, _, count := s.Stats()
	if used != 15 || count != 5 {
		t.Fatalf("stats used=%d count=%d", used, count)
	}
	tmps, _ := filepath.Glob(filepath.Join(s.chunksDir, "*", ".tmp-*"))
	if len(tmps) != 0 {
		t.Fatal("temp files left behind")
	}
}

func TestInvalidIDs(t *testing.T) {
	s := newStore(t)
	for _, bad := range []string{"../etc", "a/b", "", ".hidden"} {
		if _, err := s.Put(bad, []byte("x"), checksum.SHA256Hex([]byte("x")), "", nil); !errors.Is(err, ErrInvalidChunkID) {
			t.Fatalf("%q: want invalid id, got %v", bad, err)
		}
	}
}

func TestWorkerIDPersisted(t *testing.T) {
	dir := t.TempDir()
	if id, _ := LoadOrInitWorkerID(dir, "worker7"); id != "worker7" {
		t.Fatal(id)
	}
	if id, _ := LoadOrInitWorkerID(dir, "other"); id != "worker7" {
		t.Fatal("identity must survive restarts")
	}
}
