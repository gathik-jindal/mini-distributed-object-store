// Package worker implements a storage worker: the chunk store, its internal
// HTTP API, the register/heartbeat agent and the background scrubber.
package worker

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
)

// Layout:  <data_dir>/chunks/<ab>/<chunk_id>.bin   (ab = first 2 hex chars of sha1(chunk_id))
//          <data_dir>/chunks/<ab>/<chunk_id>.json  {checksum, size, object_id, index, stored_at}
//
// Writes are atomic: temp file -> fsync -> rename. The sidecar is renamed into
// place before the data file, so a visible .bin always has a sidecar.

var chunkIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,200}$`)

var (
	ErrInvalidChunkID = errors.New("invalid chunk id")
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrChunkConflict  = errors.New("chunk exists with a different checksum")
)

// ChecksumMismatchError is returned when the bytes don't match the declared checksum.
type ChecksumMismatchError struct{ Expected, Actual string }

func (e *ChecksumMismatchError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %s, got %s", e.Expected, e.Actual)
}

// ChunkMeta is the sidecar content.
type ChunkMeta struct {
	ChunkID  string  `json:"chunk_id"`
	Checksum string  `json:"checksum"`
	Size     int64   `json:"size"`
	ObjectID string  `json:"object_id,omitempty"`
	Index    *int    `json:"index,omitempty"`
	StoredAt float64 `json:"stored_at"`
}

// Entry is one listed chunk; Corrupt is only set by a verified listing.
type Entry struct {
	Meta    ChunkMeta
	Corrupt bool
}

type ChunkStore struct {
	root      string
	chunksDir string
	mu        sync.Mutex // serialises writers/deleters

	cmu     sync.Mutex
	corrupt map[string]struct{} // found by the scrubber, not yet rewritten
}

func NewChunkStore(dataDir string) (*ChunkStore, error) {
	cs := &ChunkStore{root: dataDir, chunksDir: filepath.Join(dataDir, "chunks"), corrupt: map[string]struct{}{}}
	if err := os.MkdirAll(cs.chunksDir, 0o755); err != nil {
		return nil, err
	}
	return cs, nil
}

func ValidateID(id string) error {
	if !chunkIDRe.MatchString(id) || strings.Contains(id, "..") {
		return fmt.Errorf("%w: %q", ErrInvalidChunkID, id)
	}
	return nil
}

func (s *ChunkStore) dir(id string) string {
	h := sha1.Sum([]byte(id))
	return filepath.Join(s.chunksDir, hex.EncodeToString(h[:])[:2])
}

// DataPath returns the path of a chunk's data file.
func (s *ChunkStore) DataPath(id string) string { return filepath.Join(s.dir(id), id+".bin") }
func (s *ChunkStore) metaPath(id string) string { return filepath.Join(s.dir(id), id+".json") }

// Put stores a chunk. It returns true when the chunk was (re)written and false
// when an identical, intact copy already existed (idempotent).
func (s *ChunkStore) Put(id string, data []byte, sum, objectID string, index *int) (bool, error) {
	if err := ValidateID(id); err != nil {
		return false, err
	}
	sum = strings.ToLower(sum)
	if actual := checksum.SHA256Hex(data); actual != sum {
		return false, &ChecksumMismatchError{Expected: sum, Actual: actual}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dpath := s.DataPath(id)
	if existing, err := s.readMeta(id); err == nil {
		if _, err := os.Stat(dpath); err == nil {
			if existing.Checksum != sum {
				return false, ErrChunkConflict
			}
			if onDisk, _, err := checksum.File(dpath); err == nil && onDisk == sum {
				s.clearCorrupt(id)
				return false, nil
			}
			// same identity but the bytes on disk are bad: repair overwrite
		}
	}
	if err := os.MkdirAll(filepath.Dir(dpath), 0o755); err != nil {
		return false, err
	}
	meta := ChunkMeta{ChunkID: id, Checksum: sum, Size: int64(len(data)), ObjectID: objectID, Index: index,
		StoredAt: float64(time.Now().UnixNano()) / 1e9}
	mb, _ := json.Marshal(meta)
	if err := atomicWrite(s.metaPath(id), mb); err != nil {
		return false, err
	}
	if err := atomicWrite(dpath, data); err != nil {
		return false, err
	}
	fsyncDir(filepath.Dir(dpath))
	s.clearCorrupt(id)
	return true, nil
}

// Get returns a chunk's bytes and metadata.
func (s *ChunkStore) Get(id string) ([]byte, ChunkMeta, error) {
	if err := ValidateID(id); err != nil {
		return nil, ChunkMeta{}, err
	}
	data, err := os.ReadFile(s.DataPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ChunkMeta{}, ErrChunkNotFound
	} else if err != nil {
		return nil, ChunkMeta{}, err
	}
	meta, err := s.readMeta(id)
	if err != nil { // sidecar lost: describe what we have
		meta = ChunkMeta{ChunkID: id, Checksum: checksum.SHA256Hex(data), Size: int64(len(data))}
	}
	return data, meta, nil
}

// Delete removes a chunk; it returns false if there was nothing to delete.
func (s *ChunkStore) Delete(id string) (bool, error) {
	if err := ValidateID(id); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, p := range []string{s.DataPath(id), s.metaPath(id)} {
		if err := os.Remove(p); err == nil {
			found = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return found, err
		}
	}
	s.clearCorrupt(id)
	return found, nil
}

// List returns every stored chunk. With verify, each one is re-hashed and
// Corrupt reports a mismatch against its sidecar checksum.
func (s *ChunkStore) List(verify bool) ([]Entry, error) {
	paths, err := filepath.Glob(filepath.Join(s.chunksDir, "*", "*.bin"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	out := make([]Entry, 0, len(paths))
	for _, p := range paths {
		id := strings.TrimSuffix(filepath.Base(p), ".bin")
		meta, merr := s.readMeta(id)
		var sum string
		var size int64
		if merr != nil || verify {
			if sum, size, err = checksum.File(p); err != nil {
				continue // deleted concurrently
			}
		}
		if merr != nil {
			st, err := os.Stat(p)
			if err != nil {
				continue
			}
			meta = ChunkMeta{ChunkID: id, Checksum: sum, Size: size, StoredAt: float64(st.ModTime().UnixNano()) / 1e9}
		}
		e := Entry{Meta: meta}
		if verify {
			e.Corrupt = sum != meta.Checksum || size != meta.Size
		}
		out = append(out, e)
	}
	return out, nil
}

// Scrub re-hashes every local chunk and remembers the corrupt ones.
func (s *ChunkStore) Scrub() (int, []string, error) {
	entries, err := s.List(true)
	if err != nil {
		return 0, nil, err
	}
	bad := []string{}
	set := map[string]struct{}{}
	for _, e := range entries {
		if e.Corrupt {
			bad = append(bad, e.Meta.ChunkID)
			set[e.Meta.ChunkID] = struct{}{}
		}
	}
	s.cmu.Lock()
	s.corrupt = set
	s.cmu.Unlock()
	return len(entries), bad, nil
}

// CorruptChunks returns chunk ids the scrubber flagged and that weren't rewritten since.
func (s *ChunkStore) CorruptChunks() []string {
	s.cmu.Lock()
	defer s.cmu.Unlock()
	out := make([]string, 0, len(s.corrupt))
	for id := range s.corrupt {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func (s *ChunkStore) clearCorrupt(id string) {
	s.cmu.Lock()
	delete(s.corrupt, id)
	s.cmu.Unlock()
}

// Stats reports used bytes, free disk space and the number of chunks.
func (s *ChunkStore) Stats() (used int64, free int64, count int) {
	paths, _ := filepath.Glob(filepath.Join(s.chunksDir, "*", "*.bin"))
	for _, p := range paths {
		if st, err := os.Stat(p); err == nil {
			used += st.Size()
			count++
		}
	}
	var fs syscall.Statfs_t
	if syscall.Statfs(s.root, &fs) == nil {
		free = int64(fs.Bavail) * int64(fs.Bsize)
	}
	return used, free, count
}

func (s *ChunkStore) readMeta(id string) (ChunkMeta, error) {
	var m ChunkMeta
	b, err := os.ReadFile(s.metaPath(id))
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	if m.Checksum == "" {
		return m, errors.New("sidecar without checksum")
	}
	m.ChunkID = id
	return m, nil
}

func atomicWrite(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func fsyncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

// LoadOrInitWorkerID persists the worker identity on its volume so it survives restarts.
func LoadOrInitWorkerID(dataDir, configured string) (string, error) {
	p := filepath.Join(dataDir, "worker_id")
	if b, err := os.ReadFile(p); err == nil {
		if id := strings.TrimSpace(string(b)); id != "" {
			return id, nil
		}
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", err
	}
	return configured, atomicWrite(p, []byte(configured))
}
