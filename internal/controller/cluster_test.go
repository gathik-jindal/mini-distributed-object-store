package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/client"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/config"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/worker"
)

// In-process cluster: a real controller and 4 real workers on httptest
// servers. down() makes a worker drop every connection, like a stopped container.

const testChunk = 64 * 1024 // small chunks keep tests fast

type flaky struct {
	h    http.Handler
	down atomic.Bool
}

func (f *flaky) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.down.Load() {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	f.h.ServeHTTP(w, r)
}

type testWorker struct {
	id    string
	store *worker.ChunkStore
	fl    *flaky
	srv   *httptest.Server
}

type testCluster struct {
	t       *testing.T
	cfg     config.Config
	ctl     *Controller
	srv     *httptest.Server
	cli     *client.Client
	workers map[string]*testWorker
	ids     []string
}

func testConfig(dir string) config.Config {
	cfg := config.Defaults()
	cfg.ChunkSize = testChunk
	cfg.SuspectAfter = 200 * time.Millisecond
	cfg.DeadAfter = 400 * time.Millisecond
	cfg.RepairGrace = 0
	cfg.OrphanMinAge = 0
	cfg.HTTPTimeout = 5 * time.Second
	cfg.DBPath = filepath.Join(dir, "ctl", "meta.db")
	return cfg
}

func newCluster(t *testing.T, dir string, mods ...func(*config.Config)) *testCluster {
	t.Helper()
	cfg := testConfig(dir)
	for _, m := range mods {
		m(&cfg)
	}
	tc := &testCluster{t: t, cfg: cfg, workers: map[string]*testWorker{}}
	for i := 1; i <= 4; i++ {
		id := "worker" + string(rune('0'+i))
		st, err := worker.NewChunkStore(filepath.Join(dir, id))
		if err != nil {
			t.Fatal(err)
		}
		fl := &flaky{h: (&worker.Server{WorkerID: id, Store: st}).Routes()}
		tw := &testWorker{id: id, store: st, fl: fl, srv: httptest.NewServer(fl)}
		tc.workers[id] = tw
		tc.ids = append(tc.ids, id)
	}
	ctl, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	tc.ctl = ctl
	tc.srv = httptest.NewServer(ctl.Handler())
	tc.cli = client.New(tc.srv.URL)
	for _, id := range tc.ids {
		tc.register(id)
	}
	tc.settle()
	t.Cleanup(tc.close)
	return tc
}

func (tc *testCluster) close() {
	tc.srv.Close()
	for _, w := range tc.workers {
		w.srv.Close()
	}
	_ = tc.ctl.Close()
}

func (tc *testCluster) post(path string, body any) *http.Response {
	b, _ := json.Marshal(body)
	resp, err := http.Post(tc.srv.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		tc.t.Fatal(err)
	}
	return resp
}

func (tc *testCluster) register(id string) {
	w := tc.workers[id]
	used, free, n := w.store.Stats()
	resp := tc.post("/internal/workers/register", api.RegisterRequest{WorkerID: id, URL: w.srv.URL,
		WorkerStats: api.WorkerStats{UsedBytes: used, FreeBytes: free, ChunkCount: n}})
	resp.Body.Close()
	if resp.StatusCode != 200 {
		tc.t.Fatalf("register %s: %d", id, resp.StatusCode)
	}
}

func (tc *testCluster) heartbeatAll() {
	for _, id := range tc.ids {
		w := tc.workers[id]
		if w.fl.down.Load() {
			continue
		}
		used, free, n := w.store.Stats()
		resp := tc.post("/internal/workers/heartbeat", api.Heartbeat{WorkerID: id,
			Stats: api.WorkerStats{UsedBytes: used, FreeBytes: free, ChunkCount: n}, CorruptChunks: w.store.CorruptChunks()})
		resp.Body.Close()
	}
}

func (tc *testCluster) settle() { tc.ctl.members.WaitReconciles() }

func (tc *testCluster) down(id string) { tc.workers[id].fl.down.Store(true) }
func (tc *testCluster) up(id string)   { tc.workers[id].fl.down.Store(false) }

// kill makes a worker unreachable and lets the failure detector declare it DEAD.
func (tc *testCluster) kill(id string) {
	tc.down(id)
	m := tc.ctl.members
	m.mu.Lock()
	m.workers[id].lastSeen = time.Now().Add(-time.Hour)
	m.mu.Unlock()
	m.CheckOnce(context.Background())
	if m.Status(id) != Dead {
		tc.t.Fatalf("%s not dead", id)
	}
}

func (tc *testCluster) upload(data []byte, name string, meta map[string]any) *api.UploadResponse {
	tc.t.Helper()
	res, err := tc.cli.Upload(context.Background(), name, bytes.NewReader(data), client.UploadOptions{Metadata: meta})
	if err != nil {
		tc.t.Fatalf("upload: %v", err)
	}
	return res
}

func (tc *testCluster) download(id string) ([]byte, error) {
	var buf bytes.Buffer
	_, err := tc.cli.Download(context.Background(), id, &buf)
	return buf.Bytes(), err
}

func (tc *testCluster) repairPass() api.RepairSummary {
	s, err := tc.ctl.repair.RunPass(context.Background())
	if err != nil {
		tc.t.Fatal(err)
	}
	return s
}

func (tc *testCluster) report() *api.ReplicationReport {
	r, err := tc.cli.Replication(context.Background(), true)
	if err != nil {
		tc.t.Fatal(err)
	}
	return r
}

func (tc *testCluster) chunkCountOnDisk() int {
	total := 0
	for _, w := range tc.workers {
		_, _, n := w.store.Stats()
		total += n
	}
	return total
}

// replicas maps chunk id -> set of "worker:STATE".
func (tc *testCluster) replicas(id string) map[string]map[string]bool {
	m, err := tc.cli.Metadata(context.Background(), id)
	if err != nil {
		tc.t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, ch := range m.Chunks {
		out[ch.ChunkID] = map[string]bool{}
		for _, r := range ch.Replicas {
			out[ch.ChunkID][r.WorkerID+":"+r.State] = true
		}
	}
	return out
}

func corruptOnDisk(t *testing.T, w *testWorker, chunkID string) {
	t.Helper()
	p := w.store.DataPath(chunkID)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func apiErr(t *testing.T, err error) *client.APIError {
	t.Helper()
	var ae *client.APIError
	if !errors.As(err, &ae) {
		t.Fatalf("want API error, got %v", err)
	}
	return ae
}

// =============================================================================== tests

func TestRoundTripSizes(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	for _, size := range []int{0, 1, testChunk, testChunk + 1, testChunk*5 + testChunk/2} {
		data := randBytes(size)
		res := tc.upload(data, "blob.bin", nil)
		want := (size + testChunk - 1) / testChunk
		if res.Size != int64(size) || res.ChunkCount != want || len(res.Chunks) != want {
			t.Fatalf("size %d: got size=%d chunks=%d", size, res.Size, res.ChunkCount)
		}
		if res.Checksum != checksum.SHA256Hex(data) {
			t.Fatal("bad checksum")
		}
		for _, ch := range res.Chunks {
			if len(ch.Workers) != 2 || ch.Workers[0] == ch.Workers[1] {
				t.Fatalf("chunk %d not on 2 distinct workers: %v", ch.Index, ch.Workers)
			}
		}
		var buf bytes.Buffer
		info, err := tc.cli.Download(context.Background(), res.ObjectID, &buf)
		if err != nil || !bytes.Equal(buf.Bytes(), data) {
			t.Fatalf("size %d: download mismatch (%v)", size, err)
		}
		if info.ETag != res.Checksum || info.Filename != "blob.bin" {
			t.Fatalf("headers: %+v", info)
		}
	}
}

func TestChunksSpreadOverAllWorkers(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(20*testChunk), "big", nil)
	used := map[string]bool{}
	for _, ch := range res.Chunks {
		for _, w := range ch.Workers {
			used[w] = true
		}
	}
	if len(used) != 4 {
		t.Fatalf("only %d workers used", len(used))
	}
	if n := tc.chunkCountOnDisk(); n != 40 {
		t.Fatalf("want 40 chunk files (20 x RF 2), got %d", n)
	}
}

func TestContentTypeGuess(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload([]byte("hi"), "notes.txt", nil)
	var buf bytes.Buffer
	info, _ := tc.cli.Download(context.Background(), res.ObjectID, &buf)
	if res.ContentType[:10] != "text/plain" || info.ContentType[:10] != "text/plain" {
		t.Fatalf("content type %q / %q", res.ContentType, info.ContentType)
	}
}

func TestUploadValidation(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	// no file field
	var body bytes.Buffer
	body.WriteString("--x\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n{}\r\n--x--\r\n")
	resp, _ := http.Post(tc.srv.URL+"/api/v1/objects", "multipart/form-data; boundary=x", &body)
	if resp.StatusCode != 400 {
		t.Fatalf("missing file: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// not multipart at all
	resp, _ = http.Post(tc.srv.URL+"/api/v1/objects", "application/json", bytes.NewReader([]byte("{}")))
	if resp.StatusCode != 400 {
		t.Fatalf("not multipart: %d", resp.StatusCode)
	}
	resp.Body.Close()
	for _, bad := range []string{"not json", "[1,2]", `{"a": {"nested": 1}}`, `{"a": null}`} {
		var b bytes.Buffer
		b.WriteString("--x\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n" + bad +
			"\r\n--x\r\nContent-Disposition: form-data; name=\"file\"; filename=\"f\"\r\n\r\nabc\r\n--x--\r\n")
		resp, _ := http.Post(tc.srv.URL+"/api/v1/objects", "multipart/form-data; boundary=x", &b)
		var env api.ErrorEnvelope
		_ = json.NewDecoder(resp.Body).Decode(&env)
		resp.Body.Close()
		if resp.StatusCode != 400 || env.Error.Code != "INVALID_REQUEST" {
			t.Fatalf("%q: %d %+v", bad, resp.StatusCode, env)
		}
	}
}

func TestMetadataGetPatch(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	ctx := context.Background()
	res := tc.upload(randBytes(3*testChunk), "a.bin", map[string]any{"owner": "alice", "project": "demo"})
	m, err := tc.cli.Metadata(ctx, res.ObjectID)
	if err != nil {
		t.Fatal(err)
	}
	if m.CustomMetadata["owner"] != "alice" || m.ChunkCount != 3 || m.Name != "a.bin" || m.LastModified == "" {
		t.Fatalf("metadata %+v", m)
	}
	for _, ch := range m.Chunks {
		if len(ch.Replicas) != 2 {
			t.Fatal("expected 2 replicas per chunk")
		}
	}
	before := tc.chunkCountOnDisk()
	time.Sleep(5 * time.Millisecond)
	p, err := tc.cli.PatchMetadata(ctx, res.ObjectID, map[string]any{"project": "final", "owner": nil, "version": 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.CustomMetadata["owner"]; ok || p.CustomMetadata["project"] != "final" || len(p.CustomMetadata) != 2 {
		t.Fatalf("patched %v", p.CustomMetadata)
	}
	if p.LastModified <= m.LastModified {
		t.Fatal("last_modified must advance")
	}
	if tc.chunkCountOnDisk() != before {
		t.Fatal("metadata update must not touch object data")
	}
	p, _ = tc.cli.PatchMetadata(ctx, res.ObjectID, map[string]any{"custom_metadata": map[string]any{"x": "y"}})
	if p.CustomMetadata["x"] != "y" {
		t.Fatal("wrapped form not accepted")
	}
	for _, body := range []string{"[1]", "{bad"} {
		req, _ := http.NewRequest(http.MethodPatch, tc.srv.URL+"/api/v1/objects/"+res.ObjectID+"/metadata", bytes.NewReader([]byte(body)))
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("%s: %d", body, resp.StatusCode)
		}
	}
	if _, err := tc.cli.PatchMetadata(ctx, "nope", map[string]any{}); apiErr(t, err).Status != 404 {
		t.Fatal("unknown object must be 404")
	}
}

func TestNotFound(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	ctx := context.Background()
	_, err := tc.download("nope")
	if ae := apiErr(t, err); ae.Status != 404 || ae.Code != "OBJECT_NOT_FOUND" {
		t.Fatal(ae)
	}
	if _, err := tc.cli.Metadata(ctx, "nope"); apiErr(t, err).Status != 404 {
		t.Fatal("metadata 404")
	}
	if err := tc.cli.Delete(ctx, "nope"); apiErr(t, err).Status != 404 {
		t.Fatal("delete 404")
	}
}

func TestListPagination(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	ctx := context.Background()
	ids := map[string]bool{}
	for i := 0; i < 5; i++ {
		ids[tc.upload(bytes.Repeat([]byte("x"), i), "f", nil).ObjectID] = true
	}
	l, err := tc.cli.List(ctx, 2, 1)
	if err != nil || l.Total != 5 || len(l.Items) != 2 || l.Limit != 2 || l.Offset != 1 {
		t.Fatalf("page: %+v %v", l, err)
	}
	all, _ := tc.cli.List(ctx, 50, 0)
	for _, it := range all.Items {
		delete(ids, it.ObjectID)
	}
	if len(ids) != 0 {
		t.Fatal("list missed objects")
	}
	for _, q := range []string{"limit=0", "limit=abc", "offset=-1"} {
		resp, _ := http.Get(tc.srv.URL + "/api/v1/objects?" + q)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("%s: %d", q, resp.StatusCode)
		}
	}
}

func TestDeleteRemovesChunks(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(4*testChunk), "d", nil)
	if err := tc.cli.Delete(context.Background(), res.ObjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.download(res.ObjectID); apiErr(t, err).Status != 404 {
		t.Fatal("deleted object must be gone")
	}
	tc.ctl.gc.FlushPendingDeletes(context.Background())
	if n := tc.chunkCountOnDisk(); n != 0 {
		t.Fatalf("%d chunk files left", n)
	}
}

func TestDeleteWhileWorkerDownFinishesLater(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(8*testChunk), "d", nil)
	tc.kill("worker3")
	_ = tc.cli.Delete(context.Background(), res.ObjectID)
	tc.ctl.gc.FlushPendingDeletes(context.Background())
	if _, _, n := tc.workers["worker3"].store.Stats(); n == 0 {
		t.Fatal("worker3 was down; its copies can't have been deleted yet")
	}
	tc.up("worker3")
	tc.register("worker3")
	tc.settle()
	tc.ctl.gc.FlushPendingDeletes(context.Background())
	if _, _, n := tc.workers["worker3"].store.Stats(); n != 0 {
		t.Fatalf("worker3 still has %d chunks", n)
	}
}

func TestInsufficientWorkers(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	for _, w := range []string{"worker1", "worker2", "worker3"} {
		tc.kill(w)
	}
	_, err := tc.cli.Upload(context.Background(), "x", bytes.NewReader([]byte("data")), client.UploadOptions{})
	if ae := apiErr(t, err); ae.Status != 503 || ae.Code != "INSUFFICIENT_WORKERS" {
		t.Fatal(ae)
	}
	if l, _ := tc.cli.List(context.Background(), 10, 0); l.Total != 0 {
		t.Fatal("failed upload must not be listed")
	}
}

func TestUploadFailsOverToNextWorker(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	tc.down("worker1") // unreachable, not yet detected as dead
	data := randBytes(10 * testChunk)
	res := tc.upload(data, "f", nil)
	for _, ch := range res.Chunks {
		if ch.Workers[0] == "worker1" || ch.Workers[1] == "worker1" || ch.Workers[0] == ch.Workers[1] {
			t.Fatalf("bad placement %v", ch.Workers)
		}
	}
	if got, err := tc.download(res.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatal("download after failover")
	}
}

func TestWorkerFailureReReplicationAndReturn(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	data := randBytes(12 * testChunk)
	res := tc.upload(data, "f", nil)
	var affected []string
	for _, ch := range res.Chunks {
		if ch.Workers[0] == "worker2" || ch.Workers[1] == "worker2" {
			affected = append(affected, ch.ChunkID)
		}
	}
	if len(affected) == 0 {
		t.Skip("placement put nothing on worker2")
	}
	tc.kill("worker2")
	if r := tc.report(); r.UnderReplicated != len(affected) {
		t.Fatalf("under-replicated %d, want %d", r.UnderReplicated, len(affected))
	}
	if got, err := tc.download(res.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatal("must still be downloadable while degraded")
	}
	if s := tc.repairPass(); s.RepairsSucceeded != len(affected) {
		t.Fatalf("repaired %d of %d", s.RepairsSucceeded, len(affected))
	}
	for cid, reps := range tc.replicas(res.ObjectID) {
		live := 0
		for k := range reps {
			if k[len(k)-3:] == ":OK" && k[:7] != "worker2" {
				live++
			}
		}
		if live != 2 {
			t.Fatalf("chunk %s has %d live copies: %v", cid, live, reps)
		}
	}
	for _, cid := range affected {
		if !tc.replicas(res.ObjectID)[cid]["worker2:STALE"] {
			t.Fatal("dead worker's copy should be STALE")
		}
	}
	if r := tc.report(); r.Healthy != 12 {
		t.Fatalf("healthy %d", r.Healthy)
	}

	// worker2 returns: stale copies verified OK -> over-replicated -> trimmed
	tc.up("worker2")
	tc.register("worker2")
	tc.settle()
	tc.repairPass()
	tc.ctl.gc.FlushPendingDeletes(context.Background())
	for cid, reps := range tc.replicas(res.ObjectID) {
		if len(reps) != 2 {
			t.Fatalf("chunk %s: %v", cid, reps)
		}
		for k := range reps {
			if k[len(k)-3:] != ":OK" {
				t.Fatalf("chunk %s: %v", cid, reps)
			}
		}
	}
	if n := tc.chunkCountOnDisk(); n != 24 {
		t.Fatalf("want 24 chunk files, got %d", n)
	}
	if got, _ := tc.download(res.ObjectID); !bytes.Equal(got, data) {
		t.Fatal("download after return")
	}
}

func TestWorkerBackWithinGraceMovesNothing(t *testing.T) {
	tc := newCluster(t, t.TempDir(), func(c *config.Config) { c.RepairGrace = time.Hour })
	res := tc.upload(randBytes(6*testChunk), "f", nil)
	before := tc.replicas(res.ObjectID)
	tc.kill("worker1")
	if s := tc.repairPass(); s.RepairsAttempted != 0 {
		t.Fatal("must wait for the grace period")
	}
	tc.up("worker1")
	tc.heartbeatAll()
	tc.settle()
	after := tc.replicas(res.ObjectID)
	for cid := range before {
		if len(before[cid]) != len(after[cid]) {
			t.Fatal("replicas changed")
		}
		for k := range before[cid] {
			if !after[cid][k] {
				t.Fatal("replicas changed")
			}
		}
	}
}

func TestTwoWorkersLost(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	data := randBytes(10 * testChunk)
	res := tc.upload(data, "f", nil)
	tc.kill("worker1")
	tc.kill("worker2")
	both := 0
	for _, ch := range res.Chunks {
		ws := append([]string(nil), ch.Workers...)
		sort.Strings(ws)
		if ws[0] == "worker1" && ws[1] == "worker2" {
			both++
		}
	}
	_, err := tc.download(res.ObjectID)
	if both > 0 {
		if ae := apiErr(t, err); ae.Status != 503 || ae.Code != "CHUNK_UNAVAILABLE" {
			t.Fatal(ae)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	tc.repairPass()
	if r := tc.report(); r.Unavailable != both || r.Healthy != 10-both {
		t.Fatalf("report %+v (both=%d)", r, both)
	}
}

func TestReadDetectsCorruptionAndRepairs(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	data := randBytes(5 * testChunk)
	res := tc.upload(data, "f", nil)
	ch := res.Chunks[2]
	ws := append([]string(nil), ch.Workers...)
	sort.Strings(ws)
	victim := ws[0] // replicas are tried in worker-id order
	corruptOnDisk(t, tc.workers[victim], ch.ChunkID)
	if got, err := tc.download(res.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatal("bad bytes must never be served")
	}
	if !tc.replicas(res.ObjectID)[ch.ChunkID][victim+":CORRUPT"] {
		t.Fatal("corrupt replica not flagged")
	}
	tc.repairPass()
	reps := tc.replicas(res.ObjectID)[ch.ChunkID]
	if !reps[ws[0]+":OK"] || !reps[ws[1]+":OK"] || len(reps) != 2 {
		t.Fatalf("not repaired in place: %v", reps)
	}
	if got, _, _ := tc.workers[victim].store.Get(ch.ChunkID); !bytes.Equal(got, data[2*testChunk:3*testChunk]) {
		t.Fatal("bytes on disk not restored")
	}
}

func TestAllReplicasCorruptReturns500(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(2*testChunk), "f", nil)
	for _, w := range res.Chunks[0].Workers {
		corruptOnDisk(t, tc.workers[w], res.Chunks[0].ChunkID)
	}
	_, err := tc.download(res.ObjectID)
	if ae := apiErr(t, err); ae.Status != 500 || ae.Code != "DATA_CORRUPTED" {
		t.Fatal(ae)
	}
}

func TestScrubDetectsWithoutRead(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(4*testChunk), "f", nil)
	ch := res.Chunks[1]
	victim := ch.Workers[1]
	corruptOnDisk(t, tc.workers[victim], ch.ChunkID)
	r, err := tc.cli.Scrub(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Workers[victim].Corrupt; len(got) != 1 || got[0] != ch.ChunkID {
		t.Fatalf("scrub found %v", got)
	}
	if r.NewlyFlagged != 1 || r.Repair == nil || r.Repair.RepairsSucceeded != 1 {
		t.Fatalf("scrub response %+v", r)
	}
	if _, bad, _ := tc.workers[victim].store.Scrub(); len(bad) != 0 {
		t.Fatal("still corrupt on disk")
	}
}

func TestHeartbeatReportsCorruption(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(3*testChunk), "f", nil)
	ch := res.Chunks[0]
	victim := ch.Workers[0]
	corruptOnDisk(t, tc.workers[victim], ch.ChunkID)
	_, _, _ = tc.workers[victim].store.Scrub() // what the background scrubber does
	tc.heartbeatAll()
	if !tc.replicas(res.ObjectID)[ch.ChunkID][victim+":CORRUPT"] {
		t.Fatal("heartbeat report not applied")
	}
	tc.repairPass()
	if !tc.replicas(res.ObjectID)[ch.ChunkID][victim+":OK"] {
		t.Fatal("not repaired")
	}
}

func TestReconcileDetectsDeletedChunkFile(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	res := tc.upload(randBytes(3*testChunk), "f", nil)
	ch := res.Chunks[0]
	victim := ch.Workers[0]
	_, _ = tc.workers[victim].store.Delete(ch.ChunkID)
	r, err := tc.cli.Reconcile(context.Background(), victim)
	if err != nil || r.Result.Missing != 1 {
		t.Fatalf("reconcile %+v %v", r, err)
	}
	tc.repairPass()
	if _, _, err := tc.workers[victim].store.Get(ch.ChunkID); err != nil {
		t.Fatal("missing chunk not restored")
	}
}

func TestOrphansRemovedOnReconcile(t *testing.T) {
	tc := newCluster(t, t.TempDir())
	_, _ = tc.workers["worker4"].store.Put("deadbeef-c00000", []byte("junk"), checksum.SHA256Hex([]byte("junk")), "", nil)
	r, err := tc.cli.Reconcile(context.Background(), "worker4")
	if err != nil || r.Result.OrphansDeleted != 1 {
		t.Fatalf("reconcile %+v %v", r, err)
	}
	if _, _, n := tc.workers["worker4"].store.Stats(); n != 0 {
		t.Fatal("orphan not deleted")
	}
}

func TestControllerRestartKeepsMetadata(t *testing.T) {
	dir := t.TempDir()
	data := randBytes(3 * testChunk)
	tc := newCluster(t, dir)
	res := tc.upload(data, "f", map[string]any{"k": "v"})
	tc.srv.Close()
	_ = tc.ctl.Close()

	// new controller process on the same DB, same workers
	ctl, err := New(context.Background(), tc.cfg)
	if err != nil {
		t.Fatal(err)
	}
	tc.ctl = ctl
	tc.srv = httptest.NewServer(ctl.Handler())
	tc.cli = client.New(tc.srv.URL)
	tc.heartbeatAll()
	tc.settle()
	if got, err := tc.download(res.ObjectID); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download after restart: %v", err)
	}
	if m, _ := tc.cli.Metadata(context.Background(), res.ObjectID); m.CustomMetadata["k"] != "v" {
		t.Fatal("custom metadata lost")
	}
}
