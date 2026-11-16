package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
)

// staleUploadAge: UPLOADING rows older than this are considered abandoned.
const staleUploadAge = 6 * time.Hour

// GC handles queued chunk deletes, orphan chunks and abandoned uploads.
type GC struct {
	c         *Controller
	wake      chan struct{}
	lastSweep time.Time
}

func newGC(c *Controller) *GC {
	return &GC{c: c, wake: make(chan struct{}, 1), lastSweep: time.Now()}
}

func (g *GC) Wake() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *GC) Run(ctx context.Context) {
	t := time.NewTicker(g.c.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-g.wake:
		}
		g.FlushPendingDeletes(ctx)
		if time.Since(g.lastSweep) >= g.c.cfg.OrphanSweepInterval {
			g.lastSweep = time.Now()
			g.SweepOrphans(ctx)
			cutoff := time.Now().Add(-staleUploadAge).UTC().Format("2006-01-02T15:04:05.000Z")
			if ids, err := g.c.repo.DropStaleUploads(ctx, cutoff); err == nil && len(ids) > 0 {
				slog.Info("STALE_UPLOADS_DROPPED", "count", len(ids))
			}
		}
	}
}

// FlushPendingDeletes sends queued deletes to every ALIVE worker.
func (g *GC) FlushPendingDeletes(ctx context.Context) int {
	c := g.c
	alive := c.members.AliveIDs()
	if len(alive) == 0 {
		return 0
	}
	pend, err := c.repo.PendingDeletes(ctx, alive, 500)
	if err != nil {
		slog.Error("gc: list pending deletes", "error", err)
		return 0
	}
	done := 0
	for _, p := range pend {
		err := c.workers.DeleteChunk(ctx, c.members.URL(p.WorkerID), p.ChunkID)
		_ = c.repo.FinishDelete(ctx, p.ChunkID, p.WorkerID, err == nil)
		if err == nil {
			done++
		}
	}
	if done > 0 {
		slog.Info("GC_DELETED", "chunks", done)
	}
	return done
}

// DeleteOrphans removes chunks a worker holds that the metadata doesn't
// reference (object deleted while the worker was down, aborted upload, trimmed copy...).
func (g *GC) DeleteOrphans(ctx context.Context, workerID, url string, inv api.Inventory, known map[string]bool) int {
	uploading, err := g.c.repo.UploadingObjectIDs(ctx)
	if err != nil {
		return 0
	}
	now, n := unixNow(), 0
	for _, it := range inv.Chunks {
		if known[it.ChunkID] || uploading[ObjectIDOf(it.ChunkID)] {
			continue
		}
		if now-it.StoredAt < g.c.cfg.OrphanMinAge.Seconds() {
			continue // may belong to an upload/repair that is about to commit
		}
		if err := g.c.workers.DeleteChunk(ctx, url, it.ChunkID); err == nil {
			n++
			slog.Info("ORPHAN_DELETED", "worker_id", workerID, "chunk_id", it.ChunkID)
		}
	}
	return n
}

// SweepOrphans checks every ALIVE worker for orphan chunks.
func (g *GC) SweepOrphans(ctx context.Context) int {
	c, total := g.c, 0
	for _, wid := range c.members.AliveIDs() {
		url := c.members.URL(wid)
		inv, err := c.workers.Inventory(ctx, url, wid, false)
		if err != nil {
			continue
		}
		rows, err := c.repo.WorkerReplicas(ctx, wid)
		if err != nil {
			continue
		}
		known := make(map[string]bool, len(rows))
		for _, r := range rows {
			known[r.ChunkID] = true
		}
		total += g.DeleteOrphans(ctx, wid, url, inv, known)
	}
	return total
}
