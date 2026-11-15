package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

// Agent registers the worker with the controller (with backoff) and then
// heartbeats forever, reporting disk stats and scrubber findings.
type Agent struct {
	WorkerID      string
	WorkerURL     string
	ControllerURL string
	Interval      time.Duration
	Store         *ChunkStore
	HTTP          *http.Client
}

func (a *Agent) stats() api.WorkerStats {
	used, free, count := a.Store.Stats()
	return api.WorkerStats{UsedBytes: used, FreeBytes: free, ChunkCount: count}
}

func (a *Agent) post(ctx context.Context, path string, body any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.ControllerURL+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return a.HTTP.Do(req)
}

// Register retries with exponential backoff until the controller accepts us.
func (a *Agent) Register(ctx context.Context) error {
	delay := 500 * time.Millisecond
	for {
		resp, err := a.post(ctx, "/internal/workers/register",
			api.RegisterRequest{WorkerID: a.WorkerID, URL: a.WorkerURL, WorkerStats: a.stats()})
		if err == nil {
			ok := resp.StatusCode == http.StatusOK
			if !ok {
				err = httpx.DecodeError(resp)
			}
			resp.Body.Close()
			if ok {
				slog.Info("REGISTERED", "worker_id", a.WorkerID, "controller", a.ControllerURL)
				return nil
			}
		}
		slog.Warn("REGISTER_RETRY", "worker_id", a.WorkerID, "error", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 5*time.Second)
	}
}

// HeartbeatOnce sends one heartbeat; a 404 means the controller forgot us.
func (a *Agent) HeartbeatOnce(ctx context.Context) error {
	resp, err := a.post(ctx, "/internal/workers/heartbeat",
		api.Heartbeat{WorkerID: a.WorkerID, Stats: a.stats(), CorruptChunks: a.Store.CorruptChunks()})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		slog.Info("REREGISTER", "worker_id", a.WorkerID)
		return a.Register(ctx)
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("heartbeat: %w", httpx.DecodeError(resp))
	}
	return nil
}

// Run registers and then heartbeats until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) {
	if err := a.Register(ctx); err != nil {
		return
	}
	t := time.NewTicker(a.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.HeartbeatOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("HEARTBEAT_FAILED", "worker_id", a.WorkerID, "error", err)
			}
		}
	}
}
