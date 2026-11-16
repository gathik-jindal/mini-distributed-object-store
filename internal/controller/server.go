// Package controller implements the controller: the public object API, the
// internal worker API, placement, failure detection, repair and GC.
package controller

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/config"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// Controller wires the metadata store, membership, worker client and the
// background loops together.
type Controller struct {
	cfg     config.Config
	db      *sql.DB
	repo    *store.Repo
	members *Membership
	workers *WorkerClient
	repair  *Repairer
	gc      *GC

	wg sync.WaitGroup
}

// New opens the metadata DB and restores state from a previous run.
func New(ctx context.Context, cfg config.Config) (*Controller, error) {
	db, err := store.Open(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	repo := store.NewRepo(db)
	dropped, err := repo.DropStaleUploads(ctx, "") // nothing can be mid-upload at boot
	if err != nil {
		db.Close()
		return nil, err
	}
	c := &Controller{cfg: cfg, db: db, repo: repo, members: NewMembership(cfg, repo), workers: NewWorkerClient(cfg.HTTPTimeout)}
	c.repair, c.gc = newRepairer(c), newGC(c)
	c.members.OnRevived = func(ctx context.Context, id string) { _, _ = c.repair.Reconcile(ctx, id) }
	c.members.OnChange = c.repair.Wake
	if err := c.members.Load(ctx); err != nil {
		db.Close()
		return nil, err
	}
	slog.Info("CONTROLLER_START", "db", cfg.DBPath, "known_workers", len(c.members.Snapshot()),
		"rf", cfg.ReplicationFactor, "chunk_size", cfg.ChunkSize, "dropped_uploads", len(dropped))
	return c, nil
}

// Start launches the failure detector, repair loop and GC loop.
func (c *Controller) Start(ctx context.Context) {
	for _, loop := range []func(context.Context){c.members.Run, c.repair.Run, c.gc.Run} {
		c.wg.Add(1)
		go func(f func(context.Context)) {
			defer c.wg.Done()
			f(ctx)
		}(loop)
	}
}

// Close waits for background loops (cancel Start's ctx first) and closes the DB.
func (c *Controller) Close() error {
	c.wg.Wait()
	c.members.Close()
	return c.db.Close()
}

// Handler returns the route table.
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	h := httpx.Handle

	// public object API
	mux.HandleFunc("POST /api/v1/objects", h(c.handleUpload))
	mux.HandleFunc("GET /api/v1/objects", h(c.handleList))
	mux.HandleFunc("GET /api/v1/objects/{object_id}", h(c.handleDownload))
	mux.HandleFunc("DELETE /api/v1/objects/{object_id}", h(c.handleDelete))
	mux.HandleFunc("GET /api/v1/objects/{object_id}/metadata", h(c.handleGetMetadata))
	mux.HandleFunc("PATCH /api/v1/objects/{object_id}/metadata", h(c.handlePatchMetadata))

	// cluster / admin (observability + manual triggers for demos)
	mux.HandleFunc("GET /api/v1/cluster/workers", h(c.handleWorkers))
	mux.HandleFunc("GET /api/v1/cluster/replication", h(c.handleReplication))
	mux.HandleFunc("POST /api/v1/cluster/repair", h(c.handleRepair))
	mux.HandleFunc("POST /api/v1/cluster/scrub", h(c.handleScrub))
	mux.HandleFunc("POST /api/v1/cluster/workers/{worker_id}/reconcile", h(c.handleReconcile))

	// internal (workers -> controller)
	mux.HandleFunc("POST /internal/workers/register", h(c.handleRegister))
	mux.HandleFunc("POST /internal/workers/heartbeat", h(c.handleHeartbeat))
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", h(c.handleExpectedChunks))

	mux.HandleFunc("GET /health", h(func(w http.ResponseWriter, r *http.Request) error {
		httpx.WriteJSON(w, 200, map[string]any{"status": "ok", "alive_workers": len(c.members.AliveIDs())})
		return nil
	}))
	mux.HandleFunc("/", httpx.NotFoundHandler())
	return mux
}
