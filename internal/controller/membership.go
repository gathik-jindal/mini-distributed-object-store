package controller

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/config"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
)

// Worker statuses.
//
//	ALIVE   heartbeat within SUSPECT_AFTER
//	SUSPECT no heartbeat for > SUSPECT_AFTER, or a request to it just failed.
//	        Not used for new placements; still tried for reads.
//	DEAD    no heartbeat for > DEAD_AFTER. Its replicas stop counting toward RF;
//	        after REPAIR_GRACE the repair loop re-replicates its chunks.
const (
	Alive   = "ALIVE"
	Suspect = "SUSPECT"
	Dead    = "DEAD"
)

type workerState struct {
	id             string
	url            string
	status         string
	lastHeartbeat  float64   // wall clock, for display/persistence
	lastSeen       time.Time // monotonic, for timeouts
	registeredAt   float64
	usedBytes      int64
	freeBytes      int64
	chunkCount     int
	deadSince      time.Time
	needsReconcile bool // verify inventory on first contact
}

// Membership is the in-memory worker registry (mutex-protected, persisted to
// the DB) plus the heartbeat-based failure detector.
type Membership struct {
	cfg  config.Config
	repo *store.Repo

	mu      sync.RWMutex
	workers map[string]*workerState

	OnRevived func(ctx context.Context, workerID string) // reconcile
	OnChange  func()                                     // wake the repair loop

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewMembership(cfg config.Config, repo *store.Repo) *Membership {
	ctx, cancel := context.WithCancel(context.Background())
	return &Membership{cfg: cfg, repo: repo, workers: map[string]*workerState{}, ctx: ctx, cancel: cancel}
}

// Load restores known workers after a controller restart. We don't know who is
// alive, so everyone starts SUSPECT with a fresh timer; heartbeats promote them.
func (m *Membership) Load(ctx context.Context) error {
	rows, err := m.repo.LoadWorkers(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, w := range rows {
		m.workers[w.WorkerID] = &workerState{id: w.WorkerID, url: w.URL, status: Suspect,
			lastHeartbeat: w.LastHeartbeat, lastSeen: now, registeredAt: w.RegisteredAt,
			usedBytes: w.UsedBytes, freeBytes: w.FreeBytes, chunkCount: w.ChunkCount, needsReconcile: true}
	}
	return nil
}

// Close stops in-flight reconciliations and waits for them.
func (m *Membership) Close() {
	m.cancel()
	m.wg.Wait()
}

// ---------------------------------------------------------------- queries

func (m *Membership) Known(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.workers[id]
	return ok
}

func (m *Membership) URL(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if w, ok := m.workers[id]; ok {
		return w.url
	}
	return ""
}

// Status returns a worker's status; unknown workers count as DEAD.
func (m *Membership) Status(id string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if w, ok := m.workers[id]; ok {
		return w.status
	}
	return Dead
}

func (m *Membership) IDs(statuses ...string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for id, w := range m.workers {
		for _, s := range statuses {
			if w.status == s {
				out = append(out, id)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

func (m *Membership) AliveIDs() []string { return m.IDs(Alive) }

// DeadPastGrace reports whether a worker has been DEAD for at least REPAIR_GRACE.
func (m *Membership) DeadPastGrace(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.workers[id]
	if !ok {
		return true
	}
	return w.status == Dead && !w.deadSince.IsZero() && time.Since(w.deadSince) >= m.cfg.RepairGrace
}

// LoadMap returns bytes used per worker (for least-loaded placement).
func (m *Membership) LoadMap() map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int64, len(m.workers))
	for id, w := range m.workers {
		out[id] = w.usedBytes
	}
	return out
}

// Snapshot returns the public view of every worker.
func (m *Membership) Snapshot() []api.WorkerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := float64(time.Now().UnixNano()) / 1e9
	out := make([]api.WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		info := api.WorkerInfo{WorkerID: w.id, URL: w.url, Status: w.status, LastHeartbeat: w.lastHeartbeat,
			UsedBytes: w.usedBytes, FreeBytes: w.freeBytes, ChunkCount: w.chunkCount}
		if w.lastHeartbeat > 0 {
			ago := float64(int((now-w.lastHeartbeat)*10)) / 10
			info.SecondsSinceHeartbeat = &ago
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkerID < out[j].WorkerID })
	return out
}

// ---------------------------------------------------------------- events

func unixNow() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// Register (re)adds a worker as ALIVE and schedules a reconciliation.
func (m *Membership) Register(ctx context.Context, req api.RegisterRequest) error {
	now := unixNow()
	m.mu.Lock()
	prev := "NEW"
	if old, ok := m.workers[req.WorkerID]; ok {
		prev = old.status
	}
	m.workers[req.WorkerID] = &workerState{id: req.WorkerID, url: req.URL, status: Alive, lastHeartbeat: now,
		lastSeen: time.Now(), registeredAt: now, usedBytes: req.UsedBytes, freeBytes: req.FreeBytes,
		chunkCount: req.ChunkCount}
	m.mu.Unlock()
	if err := m.repo.UpsertWorker(ctx, store.WorkerRow{WorkerID: req.WorkerID, URL: req.URL, Status: Alive,
		LastHeartbeat: now, RegisteredAt: now, UsedBytes: req.UsedBytes, FreeBytes: req.FreeBytes,
		ChunkCount: req.ChunkCount}); err != nil {
		return err
	}
	slog.Info("WORKER_REGISTERED", "worker_id", req.WorkerID, "url", req.URL, "previous", prev, "chunks", req.ChunkCount)
	m.revived(req.WorkerID)
	return nil
}

// Heartbeat records a heartbeat; it returns false for an unknown worker.
func (m *Membership) Heartbeat(ctx context.Context, hb api.Heartbeat) (bool, error) {
	m.mu.Lock()
	w, ok := m.workers[hb.WorkerID]
	if !ok {
		m.mu.Unlock()
		return false, nil
	}
	w.lastHeartbeat, w.lastSeen = unixNow(), time.Now()
	w.usedBytes, w.freeBytes, w.chunkCount = hb.Stats.UsedBytes, hb.Stats.FreeBytes, hb.Stats.ChunkCount
	prev := w.status
	revive, changed := false, false
	if prev != Alive {
		w.status, w.deadSince = Alive, time.Time{}
		revive = prev == Dead || w.needsReconcile
		changed = true
	} else if w.needsReconcile {
		revive = true
	}
	lastHB := w.lastHeartbeat
	m.mu.Unlock()

	if changed {
		slog.Info("WORKER_ALIVE", "worker_id", hb.WorkerID, "previous", prev)
		if err := m.repo.UpdateWorkerStatus(ctx, hb.WorkerID, Alive, lastHB); err != nil {
			return true, err
		}
	}
	if revive {
		m.revived(hb.WorkerID)
	} else if changed && m.OnChange != nil {
		m.OnChange()
	}
	return true, nil
}

func (m *Membership) revived(id string) {
	m.mu.Lock()
	if w, ok := m.workers[id]; ok {
		w.needsReconcile = false
	}
	m.mu.Unlock()
	if m.OnRevived != nil {
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.OnRevived(m.ctx, id)
		}()
	}
	if m.OnChange != nil {
		m.OnChange()
	}
}

// WaitReconciles blocks until all scheduled reconciliations have finished.
func (m *Membership) WaitReconciles() { m.wg.Wait() }

// MarkSuspect stops placing data on a worker right away after a failed request.
func (m *Membership) MarkSuspect(id, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.workers[id]; ok && w.status == Alive {
		w.status = Suspect
		slog.Warn("WORKER_SUSPECT", "worker_id", id, "reason", reason)
	}
}

// CheckOnce runs one pass of the failure detector.
func (m *Membership) CheckOnce(ctx context.Context) {
	type change struct{ id, status string }
	var changes []change
	m.mu.Lock()
	now := time.Now()
	for _, w := range m.workers {
		silent := now.Sub(w.lastSeen)
		switch {
		case w.status != Dead && silent > m.cfg.DeadAfter:
			w.status, w.deadSince, w.needsReconcile = Dead, now, true
			slog.Warn("WORKER_DEAD", "worker_id", w.id, "silent_s", silent.Round(100*time.Millisecond).Seconds(),
				"repair_grace_s", m.cfg.RepairGrace.Seconds())
			changes = append(changes, change{w.id, Dead})
		case w.status == Alive && silent > m.cfg.SuspectAfter:
			w.status = Suspect
			slog.Warn("WORKER_SUSPECT", "worker_id", w.id, "silent_s", silent.Round(100*time.Millisecond).Seconds())
			changes = append(changes, change{w.id, Suspect})
		}
	}
	m.mu.Unlock()
	for _, c := range changes {
		if err := m.repo.UpdateWorkerStatus(ctx, c.id, c.status, 0); err != nil {
			slog.Error("persist worker status", "error", err)
		}
	}
	if len(changes) > 0 && m.OnChange != nil {
		m.OnChange()
	}
}

// Run is the failure detector loop.
func (m *Membership) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.DetectorInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.CheckOnce(ctx)
		}
	}
}
