//go:build integration

// Integration tests against the live docker compose stack:
//
//	docker compose up -d --build
//	go test -tags integration ./tests/integration/...            # fast checks
//	DOS_SLOW=1 go test -tags integration ./tests/integration/... # + docker stop/start worker2
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/client"
)

const MiB = 1 << 20

func newClient(t *testing.T) *client.Client {
	t.Helper()
	url := os.Getenv("DOS_URL")
	if url == "" {
		url = "http://localhost:8000"
	}
	c := client.New(url)
	if _, err := c.Workers(context.Background()); err != nil {
		t.Skipf("compose stack not reachable at %s: %v", url, err)
	}
	return c
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func upload(t *testing.T, c *client.Client, data []byte, meta map[string]any) string {
	t.Helper()
	res, err := c.Upload(context.Background(), "t.bin", bytes.NewReader(data), client.UploadOptions{Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(context.Background(), res.ObjectID) })
	return res.ObjectID
}

func download(t *testing.T, c *client.Client, id string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := c.Download(context.Background(), id, &buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRoundTripAndDistribution(t *testing.T) {
	c := newClient(t)
	data := randBytes(5*MiB + MiB/2)
	res, err := c.Upload(context.Background(), "big.bin", bytes.NewReader(data), client.UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Delete(context.Background(), res.ObjectID)
	if res.ChunkCount != 6 || res.Chunks[5].Size != MiB/2 {
		t.Fatalf("want 6 chunks (last 512 KiB), got %d", res.ChunkCount)
	}
	for _, ch := range res.Chunks {
		if ch.Size > MiB || len(ch.Workers) != 2 || ch.Workers[0] == ch.Workers[1] {
			t.Fatalf("chunk %d: %+v", ch.Index, ch)
		}
	}
	var buf bytes.Buffer
	info, err := c.Download(context.Background(), res.ObjectID, &buf)
	if err != nil || !bytes.Equal(buf.Bytes(), data) || info.ETag != checksum.SHA256Hex(data) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestMetadata(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	id := upload(t, c, []byte("0123456789"), map[string]any{"a": "1"})
	m, err := c.PatchMetadata(ctx, id, map[string]any{"b": "2", "a": nil})
	if err != nil || len(m.CustomMetadata) != 1 || m.CustomMetadata["b"] != "2" {
		t.Fatalf("patch: %v %v", m, err)
	}
	if err := c.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Metadata(ctx, id); err == nil {
		t.Fatal("deleted object still has metadata")
	}
}

func TestCorruptionIsDetectedAndRepaired(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	data := randBytes(3 * MiB)
	id := upload(t, c, data, nil)
	m, _ := c.Metadata(ctx, id)
	ch := m.Chunks[1]
	ws := []string{ch.Replicas[0].WorkerID, ch.Replicas[1].WorkerID}
	sort.Strings(ws)
	out, err := exec.Command(filepath.Join(repoRoot(), "scripts", "corrupt_chunk.sh"), ws[0], ch.ChunkID).CombinedOutput()
	if err != nil {
		t.Fatalf("corrupt_chunk.sh: %v\n%s", err, out)
	}
	if !bytes.Equal(download(t, c, id), data) {
		t.Fatal("corrupted bytes were served")
	}
	waitFor(t, "repair", 30*time.Second, func() bool {
		m, err := c.Metadata(ctx, id)
		if err != nil {
			return false
		}
		for _, r := range m.Chunks[1].Replicas {
			if r.State != "OK" {
				return false
			}
		}
		return len(m.Chunks[1].Replicas) == 2
	})
}

func TestWorkerStopStart(t *testing.T) {
	if os.Getenv("DOS_SLOW") == "" {
		t.Skip("set DOS_SLOW=1 for the docker stop/start test")
	}
	c := newClient(t)
	ctx := context.Background()
	data := randBytes(8 * MiB)
	id := upload(t, c, data, nil)
	status := func(w string) string {
		ws, err := c.Workers(ctx)
		if err != nil {
			return ""
		}
		for _, x := range ws.Workers {
			if x.WorkerID == w {
				return x.Status
			}
		}
		return ""
	}
	healthy := func() bool {
		r, err := c.Replication(ctx, false)
		return err == nil && r.Healthy == r.TotalChunks && r.OverReplicated == 0
	}
	if err := exec.Command("docker", "stop", "worker2").Run(); err != nil {
		t.Fatal(err)
	}
	started := false
	defer func() {
		if !started {
			_ = exec.Command("docker", "start", "worker2").Run()
		}
	}()
	waitFor(t, "worker2 DEAD", 30*time.Second, func() bool { return status("worker2") == "DEAD" })
	if !bytes.Equal(download(t, c, id), data) {
		t.Fatal("download during failure")
	}
	waitFor(t, "re-replication", 60*time.Second, healthy)

	if err := exec.Command("docker", "start", "worker2").Run(); err != nil {
		t.Fatal(err)
	}
	started = true
	waitFor(t, "worker2 ALIVE", 30*time.Second, func() bool { return status("worker2") == "ALIVE" })
	time.Sleep(3 * time.Second)
	waitFor(t, "trim back to RF", 30*time.Second, healthy)
	if !bytes.Equal(download(t, c, id), data) {
		t.Fatal("download after return")
	}
}
