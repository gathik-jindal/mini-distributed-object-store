package controller

import (
	"log/slog"
	"net/http"
	"strconv"
	"sync"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// GET /api/v1/cluster/workers
func (c *Controller) handleWorkers(w http.ResponseWriter, r *http.Request) error {
	counts, err := c.repo.ReplicaCountsByWorker(r.Context())
	if err != nil {
		return err
	}
	ws := c.members.Snapshot()
	for i := range ws {
		ws[i].ReplicasOK = counts[ws[i].WorkerID]
	}
	httpx.WriteJSON(w, 200, api.WorkersResponse{Workers: ws, Alive: len(c.members.AliveIDs()),
		ReplicationFactor: c.cfg.ReplicationFactor})
	return nil
}

// GET /api/v1/cluster/replication?details=true&limit=200
func (c *Controller) handleReplication(w http.ResponseWriter, r *http.Request) error {
	details := r.URL.Query().Get("details") != "false"
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			return httpx.InvalidRequest("limit must be between 1 and 10000")
		}
		limit = n
	}
	rep, err := c.repair.Report(r.Context(), details, limit)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, rep)
	return nil
}

// POST /api/v1/cluster/repair -- force a repair pass now.
func (c *Controller) handleRepair(w http.ResponseWriter, r *http.Request) error {
	s, err := c.repair.RunPass(r.Context())
	if err != nil {
		return err
	}
	c.gc.FlushPendingDeletes(r.Context())
	rep, err := c.repair.Report(r.Context(), false, 0)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, api.RepairResponse{Summary: s, Report: rep})
	return nil
}

// POST /api/v1/cluster/scrub -- every alive worker re-hashes its chunks now;
// corrupt replicas are flagged immediately and repaired.
func (c *Controller) handleScrub(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	alive := c.members.AliveIDs()
	results := make([]api.ScrubWorkerResult, len(alive))
	var wg sync.WaitGroup
	for i, wid := range alive {
		wg.Add(1)
		go func(i int, wid string) {
			defer wg.Done()
			res, err := c.workers.Scrub(ctx, c.members.URL(wid))
			if err != nil {
				results[i] = api.ScrubWorkerResult{ScrubResult: api.ScrubResult{WorkerID: wid}, Error: err.Error()}
				return
			}
			results[i] = api.ScrubWorkerResult{ScrubResult: res}
		}(i, wid)
	}
	wg.Wait()
	out := api.ScrubResponse{Workers: map[string]api.ScrubWorkerResult{}}
	for i, wid := range alive {
		out.Workers[wid] = results[i]
		for _, cid := range results[i].Corrupt {
			if ok, _ := c.repo.SetReplicaState(ctx, cid, wid, store.Corrupt, store.OK, store.Stale); ok {
				out.NewlyFlagged++
				slog.Warn("REPLICA_CORRUPT", "chunk_id", cid, "worker_id", wid, "source", "scrub")
			}
		}
	}
	if out.NewlyFlagged > 0 {
		s, err := c.repair.RunPass(ctx)
		if err != nil {
			return err
		}
		out.Repair = &s
	}
	httpx.WriteJSON(w, 200, out)
	return nil
}

// POST /api/v1/cluster/workers/{worker_id}/reconcile
func (c *Controller) handleReconcile(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("worker_id")
	if !c.members.Known(id) {
		return httpx.NotFound("unknown worker %s", id)
	}
	res, err := c.repair.Reconcile(r.Context(), id)
	if err != nil {
		return httpx.ChunkUnavailable("reconcile %s: %v", id, err)
	}
	httpx.WriteJSON(w, 200, api.ReconcileResponse{WorkerID: id, Result: res})
	return nil
}
