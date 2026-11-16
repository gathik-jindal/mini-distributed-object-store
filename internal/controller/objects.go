package controller

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

const (
	maxMetaKeys  = 100
	maxKeyLen    = 128
	maxValueLen  = 4096
	multipartMem = 8 << 20 // larger uploads are spooled to disk by net/http
)

// ChunkIDFor builds the unique, order-preserving chunk id.
func ChunkIDFor(objectID string, index int) string { return fmt.Sprintf("%s-c%05d", objectID, index) }

// ObjectIDOf recovers the object id from a chunk id.
func ObjectIDOf(chunkID string) string {
	if i := strings.LastIndex(chunkID, "-c"); i >= 0 {
		return chunkID[:i]
	}
	return chunkID
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func guessContentType(declared, name string) string {
	if declared != "" && declared != "application/octet-stream" {
		return declared
	}
	if t := mime.TypeByExtension(strings.ToLower(filepath.Ext(name))); t != "" {
		return t
	}
	return "application/octet-stream"
}

// validateCustomMetadata checks a key/value map; nulls are allowed only for PATCH (removal).
func validateCustomMetadata(v any, allowNull bool) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, httpx.InvalidRequest("metadata must be a JSON object of key/value pairs")
	}
	if len(m) > maxMetaKeys {
		return nil, httpx.InvalidRequest("at most %d metadata keys allowed", maxMetaKeys)
	}
	for k, val := range m {
		if k == "" || len(k) > maxKeyLen {
			return nil, httpx.InvalidRequest("metadata keys must be 1..%d characters", maxKeyLen).WithDetails(map[string]any{"key": k})
		}
		switch x := val.(type) {
		case nil:
			if !allowNull {
				return nil, httpx.InvalidRequest("metadata values must be strings, numbers or booleans").WithDetails(map[string]any{"key": k, "type": "null"})
			}
		case string:
			if len(x) > maxValueLen {
				return nil, httpx.InvalidRequest("metadata values must be at most %d characters", maxValueLen).WithDetails(map[string]any{"key": k})
			}
		case json.Number, bool, float64:
		default:
			return nil, httpx.InvalidRequest("metadata values must be strings, numbers or booleans").WithDetails(map[string]any{"key": k, "type": fmt.Sprintf("%T", val)})
		}
	}
	return m, nil
}

func parseMetadataField(values []string) (map[string]any, error) {
	if len(values) == 0 || strings.TrimSpace(values[0]) == "" {
		return map[string]any{}, nil
	}
	dec := json.NewDecoder(strings.NewReader(values[0]))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, httpx.InvalidRequest("metadata is not valid JSON: %v", err)
	}
	return validateCustomMetadata(v, false)
}

func storeErr(err error, objectID string) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return httpx.ObjectNotFound(objectID)
	case errors.Is(err, store.ErrBusy):
		return httpx.ObjectBusy("%v", err)
	}
	return err
}

// =============================================================================== handlers

// POST /api/v1/objects  (multipart/form-data: file, optional metadata JSON)
func (c *Controller) handleUpload(w http.ResponseWriter, r *http.Request) error {
	if err := r.ParseMultipartForm(multipartMem); err != nil {
		return httpx.InvalidRequest("expected multipart/form-data: %v", err)
	}
	defer r.MultipartForm.RemoveAll()
	files := r.MultipartForm.File["file"]
	if len(files) == 0 {
		return httpx.InvalidRequest("missing required field 'file'")
	}
	meta, err := parseMetadataField(r.MultipartForm.Value["metadata"])
	if err != nil {
		return err
	}
	fh := files[0]
	f, err := fh.Open()
	if err != nil {
		return httpx.InvalidRequest("cannot read file part: %v", err)
	}
	defer f.Close()
	res, err := c.UploadObject(r.Context(), f, fh.Filename, fh.Header.Get("Content-Type"), meta)
	if err != nil {
		return err
	}
	w.Header().Set("Location", "/api/v1/objects/"+res.ObjectID)
	httpx.WriteJSON(w, http.StatusCreated, res)
	return nil
}

// GET /api/v1/objects?limit=&offset=
func (c *Controller) handleList(w http.ResponseWriter, r *http.Request) error {
	limit, offset := 50, 0
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			return httpx.InvalidRequest("limit must be an integer between 1 and 1000")
		}
		limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return httpx.InvalidRequest("offset must be a non-negative integer")
		}
		offset = n
	}
	items, total, err := c.repo.ListObjects(r.Context(), limit, offset)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, api.ListResponse{Items: items, Total: total, Limit: limit, Offset: offset})
	return nil
}

// GET /api/v1/objects/{object_id}
func (c *Controller) handleDownload(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("object_id")
	obj, chunks, first, err := c.prepareDownload(r.Context(), id)
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("Content-Type", obj.ContentType)
	h.Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	h.Set("ETag", `"`+obj.Checksum+`"`)
	h.Set("X-Object-Checksum-SHA256", obj.Checksum)
	h.Set("Content-Disposition", "attachment; filename*=UTF-8''"+url.PathEscape(obj.Name))
	w.WriteHeader(http.StatusOK)
	c.streamChunks(r.Context(), w, id, chunks, first)
	return nil
}

// GET /api/v1/objects/{object_id}/metadata
func (c *Controller) handleGetMetadata(w http.ResponseWriter, r *http.Request) error {
	m, err := c.objectMeta(r.Context(), r.PathValue("object_id"))
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, m)
	return nil
}

// PATCH /api/v1/objects/{object_id}/metadata
// JSON merge-patch of custom metadata: {"k": "v"} sets, {"k": null} removes.
// A body of the form {"custom_metadata": {...}} is also accepted.
func (c *Controller) handlePatchMetadata(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("object_id")
	var body any
	if err := httpx.ReadJSON(r, &body); err != nil {
		return err
	}
	if m, ok := body.(map[string]any); ok && len(m) == 1 {
		if inner, ok := m["custom_metadata"].(map[string]any); ok {
			body = inner
		}
	}
	patch, err := validateCustomMetadata(body, true)
	if err != nil {
		return err
	}
	if _, err := c.repo.PatchCustomMetadata(r.Context(), id, patch); err != nil {
		return storeErr(err, id)
	}
	m, err := c.objectMeta(r.Context(), id)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, m)
	return nil
}

// DELETE /api/v1/objects/{object_id}
func (c *Controller) handleDelete(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("object_id")
	n, err := c.repo.DeleteObject(r.Context(), id)
	if err != nil {
		return storeErr(err, id)
	}
	slog.Info("OBJECT_DELETED", "object_id", id, "queued_chunk_deletes", n)
	c.gc.Wake()
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (c *Controller) objectMeta(ctx context.Context, id string) (api.ObjectMeta, error) {
	obj, err := c.repo.GetObject(ctx, id)
	if err != nil {
		return api.ObjectMeta{}, err
	}
	if obj == nil || obj.Status != store.Committed {
		return api.ObjectMeta{}, httpx.ObjectNotFound(id)
	}
	chunks, err := c.repo.ObjectChunks(ctx, id)
	if err != nil {
		return api.ObjectMeta{}, err
	}
	infos := make([]api.ChunkInfo, 0, len(chunks))
	for _, ch := range chunks {
		infos = append(infos, api.ChunkInfo{ChunkID: ch.ChunkID, Index: ch.Index, Size: ch.Size,
			Checksum: ch.Checksum, Replicas: describeReplicas(c.members, ch.Replicas)})
	}
	return api.ObjectMeta{ObjectID: obj.ObjectID, Name: obj.Name, Size: obj.Size, ContentType: obj.ContentType,
		Checksum: obj.Checksum, ChecksumAlgorithm: "sha256", Status: obj.Status, ChunkSize: obj.ChunkSize,
		ChunkCount: obj.ChunkCount, ReplicationFactor: c.cfg.ReplicationFactor, CreatedAt: obj.CreatedAt,
		UpdatedAt: obj.UpdatedAt, LastModified: obj.UpdatedAt, CustomMetadata: obj.CustomMetadata, Chunks: infos}, nil
}

// =============================================================================== upload

// UploadObject chunks the stream, writes every chunk to RF workers and commits
// the metadata in one transaction. The whole file is never held in memory.
func (c *Controller) UploadObject(ctx context.Context, f io.Reader, name, declaredType string, meta map[string]any) (*api.UploadResponse, error) {
	rf := c.cfg.ReplicationFactor
	if alive := c.members.AliveIDs(); len(alive) < rf {
		return nil, httpx.InsufficientWorkers("need %d alive workers, have %d", rf, len(alive)).
			WithDetails(map[string]any{"alive": alive})
	}
	objectID := newUUID()
	if name == "" {
		name = objectID
	}
	contentType := guessContentType(declaredType, name)
	if err := c.repo.CreateObject(ctx, objectID, name, contentType, c.cfg.ChunkSize, meta); err != nil {
		return nil, err
	}
	slog.Info("UPLOAD_START", "object_id", objectID, "name", name)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu         sync.Mutex
		wg         sync.WaitGroup
		firstErr   error
		written    [][2]string
		rows       []store.ChunkRow
		placements = map[string][]string{}
		hasher     = checksum.New()
		size       int64
		sem        = make(chan struct{}, max(1, c.cfg.UploadParallelism))
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}
	record := func(cid, w string) {
		mu.Lock()
		written = append(written, [2]string{cid, w})
		mu.Unlock()
	}
	for index := 0; ; index++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		buf := make([]byte, c.cfg.ChunkSize)
		n, rerr := io.ReadFull(f, buf)
		if n == 0 {
			<-sem
			if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
				fail(httpx.InvalidRequest("reading upload: %v", rerr))
			}
			break
		}
		data := buf[:n]
		hasher.Write(data)
		size += int64(n)
		sum := checksum.SHA256Hex(data)
		cid := ChunkIDFor(objectID, index)
		rows = append(rows, store.ChunkRow{ChunkID: cid, ObjectID: objectID, Index: index, Size: int64(n), Checksum: sum})
		wg.Add(1)
		go func(index int, cid string, data []byte, sum string) {
			defer wg.Done()
			defer func() { <-sem }()
			ws, err := c.storeChunk(ctx, objectID, index, cid, data, sum, record)
			if err != nil {
				fail(err)
				return
			}
			mu.Lock()
			placements[cid] = ws
			mu.Unlock()
		}(index, cid, data, sum)
		if rerr != nil { // short final chunk
			if rerr != io.ErrUnexpectedEOF {
				fail(httpx.InvalidRequest("reading upload: %v", rerr))
			}
			break
		}
	}
	wg.Wait()

	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	if firstErr != nil {
		c.abortUpload(objectID, written)
		slog.Warn("UPLOAD_ABORTED", "object_id", objectID, "error", firstErr)
		var ae *httpx.APIError
		if errors.As(firstErr, &ae) {
			return nil, ae
		}
		return nil, httpx.Internal("upload failed: %v", firstErr)
	}
	sum := checksum.Hex(hasher)
	if err := c.repo.CommitObject(context.WithoutCancel(ctx), objectID, size, sum, rows, placements); err != nil {
		c.abortUpload(objectID, written)
		return nil, err
	}
	obj, err := c.repo.GetObject(context.WithoutCancel(ctx), objectID)
	if err != nil || obj == nil {
		return nil, httpx.Internal("object vanished after commit")
	}
	slog.Info("UPLOAD_COMMITTED", "object_id", objectID, "name", name, "size", size, "chunks", len(rows), "checksum", sum)
	resp := &api.UploadResponse{ObjectID: objectID, Name: name, Size: size, ContentType: contentType, Checksum: sum,
		ChunkSize: c.cfg.ChunkSize, ChunkCount: len(rows), Status: store.Committed, CreatedAt: obj.CreatedAt,
		LastModified: obj.UpdatedAt, CustomMetadata: obj.CustomMetadata, Chunks: make([]api.ChunkPlacement, 0, len(rows))}
	for _, row := range rows {
		resp.Chunks = append(resp.Chunks, api.ChunkPlacement{ChunkID: row.ChunkID, Index: row.Index, Size: row.Size,
			Checksum: row.Checksum, Workers: placements[row.ChunkID]})
	}
	return resp, nil
}

// storeChunk writes one chunk to RF distinct workers, falling back down the HRW ranking.
func (c *Controller) storeChunk(ctx context.Context, objectID string, index int, cid string, data []byte, sum string,
	record func(cid, worker string)) ([]string, error) {
	rf := c.cfg.ReplicationFactor
	candidates := RankWorkers(cid, c.members.AliveIDs())
	var placed, errs []string
	for len(placed) < rf && len(candidates) > 0 {
		need := min(rf-len(placed), len(candidates))
		batch := candidates[:need]
		candidates = candidates[need:]
		results := make([]error, len(batch))
		var wg sync.WaitGroup
		for i, w := range batch {
			wg.Add(1)
			go func(i int, w string) {
				defer wg.Done()
				results[i] = c.workers.PutChunk(ctx, c.members.URL(w), cid, data, sum, objectID, index)
			}(i, w)
		}
		wg.Wait()
		for i, w := range batch {
			if err := results[i]; err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				errs = append(errs, fmt.Sprintf("%s: %v", w, err))
				if !errors.Is(err, ErrChunkRejected) {
					c.members.MarkSuspect(w, "chunk write failed")
				}
				slog.Warn("REPLICA_WRITE_FAILED", "chunk_id", cid, "worker_id", w, "error", err)
				continue
			}
			placed = append(placed, w)
			record(cid, w)
		}
	}
	if len(placed) < rf {
		return nil, httpx.InsufficientWorkers("could only store %d/%d replicas of chunk %d", len(placed), rf, index).
			WithDetails(map[string]any{"chunk_id": cid, "errors": errs})
	}
	return placed, nil
}

func (c *Controller) abortUpload(objectID string, written [][2]string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, cw := range written {
		if err := c.workers.DeleteChunk(ctx, c.members.URL(cw[1]), cw[0]); err != nil {
			_ = c.repo.QueueDelete(ctx, cw[0], cw[1]) // GC retries later
		}
	}
	_ = c.repo.AbortObject(ctx, objectID)
}

// =============================================================================== download

// readOrder lists replicas to try, best first: OK on ALIVE, OK on SUSPECT, then
// STALE copies (checksum-verified, so a stale-but-intact copy is a safe last resort).
func (c *Controller) readOrder(ch store.Chunk) []string {
	prio := map[[2]string]int{{store.OK, Alive}: 1, {store.OK, Suspect}: 2, {store.Stale, Alive}: 3, {store.Stale, Suspect}: 4}
	type cand struct {
		w string
		p int
	}
	var cs []cand
	for _, rep := range ch.Replicas {
		if p := prio[[2]string{rep.State, c.members.Status(rep.WorkerID)}]; p > 0 {
			cs = append(cs, cand{rep.WorkerID, p})
		}
	}
	for i := 1; i < len(cs); i++ { // stable insertion sort; replicas are already in worker-id order
		for j := i; j > 0 && cs[j].p < cs[j-1].p; j-- {
			cs[j], cs[j-1] = cs[j-1], cs[j]
		}
	}
	out := make([]string, len(cs))
	for i, x := range cs {
		out[i] = x.w
	}
	return out
}

// fetchChunk fetches one chunk and verifies its sha256 against the controller's
// checksum. Corrupt or missing replicas are flagged and repaired; bad bytes are
// never returned.
func (c *Controller) fetchChunk(ctx context.Context, ch store.Chunk) ([]byte, error) {
	bg := context.WithoutCancel(ctx) // record findings even if the client went away
	triedCorrupt, triedOther := 0, 0
	for _, w := range c.readOrder(ch) {
		data, err := c.workers.GetChunk(ctx, c.members.URL(w), ch.ChunkID)
		if errors.Is(err, ErrChunkMissing) {
			triedOther++
			_, _ = c.repo.SetReplicaState(bg, ch.ChunkID, w, store.Missing, store.OK, store.Stale)
			slog.Warn("REPLICA_MISSING", "chunk_id", ch.ChunkID, "worker_id", w, "source", "read")
			c.repair.Wake()
			continue
		} else if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			triedOther++
			c.members.MarkSuspect(w, "chunk read failed")
			slog.Warn("REPLICA_READ_FAILED", "chunk_id", ch.ChunkID, "worker_id", w, "error", err)
			continue
		}
		if actual := checksum.SHA256Hex(data); actual != ch.Checksum || int64(len(data)) != ch.Size {
			triedCorrupt++
			_, _ = c.repo.SetReplicaState(bg, ch.ChunkID, w, store.Corrupt, store.OK, store.Stale)
			slog.Warn("REPLICA_CORRUPT", "chunk_id", ch.ChunkID, "worker_id", w, "source", "read",
				"expected", ch.Checksum[:16], "actual", actual[:16])
			c.repair.Wake() // read-repair
			continue
		}
		return data, nil
	}
	if triedCorrupt > 0 && triedOther == 0 {
		return nil, httpx.DataCorrupted("all replicas of chunk %d are corrupted", ch.Index).
			WithDetails(map[string]any{"chunk_id": ch.ChunkID})
	}
	return nil, httpx.ChunkUnavailable("no readable replica of chunk %d", ch.Index).
		WithDetails(map[string]any{"chunk_id": ch.ChunkID})
}

func (c *Controller) prepareDownload(ctx context.Context, id string) (*store.Object, []store.Chunk, []byte, error) {
	obj, err := c.repo.GetObject(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	if obj == nil || obj.Status != store.Committed {
		return nil, nil, nil, httpx.ObjectNotFound(id)
	}
	chunks, err := c.repo.ObjectChunks(ctx, id)
	if err != nil {
		return nil, nil, nil, err
	}
	// pre-flight: every chunk needs a replica we can plausibly read
	for _, ch := range chunks {
		if len(c.readOrder(ch)) > 0 {
			continue
		}
		allCorrupt := len(ch.Replicas) > 0
		for _, rep := range ch.Replicas {
			if rep.State != store.Corrupt {
				allCorrupt = false
			}
		}
		if allCorrupt {
			return nil, nil, nil, httpx.DataCorrupted("all replicas of chunk %d are corrupted", ch.Index).
				WithDetails(map[string]any{"chunk_id": ch.ChunkID})
		}
		return nil, nil, nil, httpx.ChunkUnavailable("chunk %d has no replica on a live worker", ch.Index).
			WithDetails(map[string]any{"chunk_id": ch.ChunkID, "replicas": describeReplicas(c.members, ch.Replicas)})
	}
	// fetch + verify chunk 0 before headers go out, so total failure is a clean error
	var first []byte
	if len(chunks) > 0 {
		if first, err = c.fetchChunk(ctx, chunks[0]); err != nil {
			return nil, nil, nil, err
		}
	}
	return obj, chunks, first, nil
}

type fetched struct {
	data []byte
	err  error
}

// streamChunks writes chunks in order, prefetching one ahead. If a later chunk
// can't be served the connection is aborted (the client sees a short body).
func (c *Controller) streamChunks(ctx context.Context, w http.ResponseWriter, objectID string, chunks []store.Chunk, first []byte) {
	if len(chunks) == 0 {
		return
	}
	start := func(i int) chan fetched {
		ch := make(chan fetched, 1)
		go func() {
			d, err := c.fetchChunk(ctx, chunks[i])
			ch <- fetched{d, err}
		}()
		return ch
	}
	var next chan fetched
	if len(chunks) > 1 {
		next = start(1)
	}
	if _, err := w.Write(first); err != nil {
		return
	}
	for i := 1; i < len(chunks); i++ {
		res := <-next
		if i+1 < len(chunks) {
			next = start(i + 1)
		}
		if res.err != nil {
			slog.Error("DOWNLOAD_ABORTED", "object_id", objectID, "chunk", i, "error", res.err)
			panic(http.ErrAbortHandler)
		}
		if _, err := w.Write(res.data); err != nil {
			return // client went away
		}
	}
}
