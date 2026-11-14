package worker

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// MaxChunkBody bounds a single PUT body.
const MaxChunkBody = 64 << 20

// Server is the worker's internal HTTP API (controller -> worker).
type Server struct {
	WorkerID string
	Store    *ChunkStore
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", httpx.Handle(s.health))
	mux.HandleFunc("PUT /internal/chunks/{chunk_id}", httpx.Handle(s.putChunk))
	mux.HandleFunc("GET /internal/chunks/{chunk_id}", httpx.Handle(s.getChunk))
	mux.HandleFunc("DELETE /internal/chunks/{chunk_id}", httpx.Handle(s.deleteChunk))
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", httpx.Handle(s.inventory))
	mux.HandleFunc("POST /internal/scrub", httpx.Handle(s.scrub))
	mux.HandleFunc("/", httpx.NotFoundHandler())
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) error {
	httpx.WriteJSON(w, 200, map[string]string{"status": "ok", "worker_id": s.WorkerID})
	return nil
}

func storeErr(err error) error {
	var mm *ChecksumMismatchError
	switch {
	case errors.Is(err, ErrInvalidChunkID):
		return httpx.InvalidRequest("%v", err)
	case errors.As(err, &mm):
		return httpx.InvalidRequest("%v", err).WithDetails(map[string]any{"expected": mm.Expected, "actual": mm.Actual})
	case errors.Is(err, ErrChunkConflict):
		return httpx.Conflict("%v", err)
	case errors.Is(err, ErrChunkNotFound):
		return httpx.NotFound("%v", err)
	}
	return err
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("chunk_id")
	sum := r.Header.Get("X-Chunk-Checksum")
	if sum == "" {
		return httpx.InvalidRequest("missing X-Chunk-Checksum header")
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, MaxChunkBody+1))
	if err != nil {
		return httpx.InvalidRequest("read body: %v", err)
	}
	if len(data) > MaxChunkBody {
		return httpx.InvalidRequest("chunk larger than %d bytes", MaxChunkBody)
	}
	var index *int
	if v := r.Header.Get("X-Chunk-Index"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			index = &n
		}
	}
	created, err := s.Store.Put(id, data, sum, r.Header.Get("X-Object-Id"), index)
	if err != nil {
		return storeErr(err)
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	httpx.WriteJSON(w, status, api.PutChunkResponse{ChunkID: id, Size: len(data), Checksum: sum, Created: created})
	return nil
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) error {
	data, meta, err := s.Store.Get(r.PathValue("chunk_id"))
	if err != nil {
		return storeErr(err)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("X-Chunk-Checksum", meta.Checksum)
	w.WriteHeader(200)
	_, _ = w.Write(data)
	return nil
}

func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) error {
	found, err := s.Store.Delete(r.PathValue("chunk_id"))
	if err != nil {
		return storeErr(err)
	}
	if !found {
		return httpx.NotFound("chunk %s not found", r.PathValue("chunk_id"))
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) inventory(w http.ResponseWriter, r *http.Request) error {
	if wid := r.PathValue("worker_id"); wid != s.WorkerID {
		return httpx.NotFound("this is %s, not %s", s.WorkerID, wid)
	}
	verify := r.URL.Query().Get("verify") == "true"
	entries, err := s.Store.List(verify)
	if err != nil {
		return err
	}
	inv := api.Inventory{WorkerID: s.WorkerID, Verified: verify, Chunks: make([]api.InventoryItem, 0, len(entries))}
	for _, e := range entries {
		inv.Chunks = append(inv.Chunks, api.InventoryItem{ChunkID: e.Meta.ChunkID, Size: e.Meta.Size,
			Checksum: e.Meta.Checksum, StoredAt: e.Meta.StoredAt, Corrupt: e.Corrupt})
	}
	httpx.WriteJSON(w, 200, inv)
	return nil
}

func (s *Server) scrub(w http.ResponseWriter, r *http.Request) error {
	res, err := RunScrub(s.WorkerID, s.Store)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, res)
	return nil
}

// RunScrub re-hashes every chunk and logs what it found.
func RunScrub(workerID string, store *ChunkStore) (api.ScrubResult, error) {
	t0 := time.Now()
	scanned, bad, err := store.Scrub()
	if err != nil {
		return api.ScrubResult{}, err
	}
	ms := time.Since(t0).Milliseconds()
	for _, id := range bad {
		slog.Warn("SCRUB_CORRUPT", "worker_id", workerID, "chunk_id", id)
	}
	slog.Info("SCRUB_DONE", "worker_id", workerID, "scanned", scanned, "corrupt", len(bad), "duration_ms", ms)
	return api.ScrubResult{WorkerID: workerID, Scanned: scanned, Corrupt: bad, DurationMS: ms}, nil
}
