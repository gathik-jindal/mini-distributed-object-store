package controller

import (
	"log/slog"
	"net/http"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// POST /internal/workers/register
func (c *Controller) handleRegister(w http.ResponseWriter, r *http.Request) error {
	var req api.RegisterRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		return err
	}
	if req.WorkerID == "" || req.URL == "" {
		return httpx.InvalidRequest("worker_id and url are required")
	}
	if err := c.members.Register(r.Context(), req); err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, api.RegisterResponse{Status: "registered", WorkerID: req.WorkerID,
		HeartbeatInterval: c.cfg.HeartbeatInterval.Seconds()})
	return nil
}

// POST /internal/workers/heartbeat
func (c *Controller) handleHeartbeat(w http.ResponseWriter, r *http.Request) error {
	var hb api.Heartbeat
	if err := httpx.ReadJSON(r, &hb); err != nil {
		return err
	}
	known, err := c.members.Heartbeat(r.Context(), hb)
	if err != nil {
		return err
	}
	if !known {
		return httpx.NotFound("unknown worker %s; register first", hb.WorkerID)
	}
	flagged := 0
	for _, cid := range hb.CorruptChunks {
		if ok, _ := c.repo.SetReplicaState(r.Context(), cid, hb.WorkerID, store.Corrupt, store.OK, store.Stale); ok {
			flagged++
			slog.Warn("REPLICA_CORRUPT", "chunk_id", cid, "worker_id", hb.WorkerID, "source", "scrubber")
		}
	}
	if flagged > 0 {
		c.repair.Wake()
	}
	httpx.WriteJSON(w, 200, map[string]string{"status": "ok"})
	return nil
}

// GET /internal/workers/{worker_id}/chunks -- metadata view: what this worker *should* hold.
func (c *Controller) handleExpectedChunks(w http.ResponseWriter, r *http.Request) error {
	id := r.PathValue("worker_id")
	if !c.members.Known(id) {
		return httpx.NotFound("unknown worker %s", id)
	}
	rows, err := c.repo.WorkerReplicas(r.Context(), id)
	if err != nil {
		return err
	}
	httpx.WriteJSON(w, 200, api.ExpectedChunks{WorkerID: id, Status: c.members.Status(id), Count: len(rows), Chunks: rows})
	return nil
}
