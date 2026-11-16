package controller

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/checksum"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller/store"
)

// Replication maintenance.
//
// A replica *counts* toward the replication factor (RF) when its state is OK and
// its worker is ALIVE/SUSPECT, or DEAD but still inside REPAIR_GRACE (we wait a
// little before moving data, in case the worker is only restarting).
//
// Each pass, per chunk:
//   - fewer than RF counted -> copy from a verified good replica to a target:
//     first overwrite a bad (CORRUPT/MISSING/STALE) copy on an ALIVE worker
//     in place, otherwise the least-loaded ALIVE worker not holding the chunk.
//     Replicas on workers dead past the grace period are then marked STALE.
//   - more than RF good -> trim extras (most-loaded worker first).
//   - healthy chunk with leftover non-OK rows on ALIVE workers -> drop + delete copy.
//
// Reconciliation (a worker (re)joins): diff its verified inventory against the DB.

type Repairer struct {
	c                 *Controller
	wake              chan struct{}
	mu                sync.Mutex // one pass at a time
	unavailableLogged map[string]bool

	smu         sync.Mutex
	lastSummary *api.RepairSummary
}

func newRepairer(c *Controller) *Repairer {
	return &Repairer{c: c, wake: make(chan struct{}, 1), unavailableLogged: map[string]bool{}}
}

// Wake asks for a repair pass as soon as possible.
func (r *Repairer) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Repairer) Run(ctx context.Context) {
	t := time.NewTicker(r.c.cfg.RepairInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.wake:
		}
		s, err := r.RunPass(ctx)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("repair pass failed", "error", err)
			}
			continue
		}
		if s.RepairsAttempted > 0 && s.RepairsSucceeded < s.RepairsAttempted {
			time.AfterFunc(time.Second, r.Wake) // some sources turned out bad; go again soon
		}
	}
}

type classified struct {
	good, grace, deadOld, badAlive []store.Replica
	counted                        int
}

func (r *Repairer) classify(ch store.Chunk) classified {
	m := r.c.members
	var k classified
	for _, rep := range ch.Replicas {
		st := m.Status(rep.WorkerID)
		switch {
		case rep.State == store.OK && (st == Alive || st == Suspect):
			k.good = append(k.good, rep)
		case rep.State == store.OK && st == Dead && !m.DeadPastGrace(rep.WorkerID):
			k.grace = append(k.grace, rep)
		case rep.State == store.OK && st == Dead:
			k.deadOld = append(k.deadOld, rep)
		case (rep.State == store.Corrupt || rep.State == store.Missing || rep.State == store.Stale) && st == Alive:
			k.badAlive = append(k.badAlive, rep)
		}
	}
	k.counted = len(k.good) + len(k.grace)
	return k
}

type repairJob struct {
	chunk   store.Chunk
	sources []string
	target  string
	deadOld []string
}

// RunPass performs one full replication-maintenance pass.
func (r *Repairer) RunPass(ctx context.Context) (api.RepairSummary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, m, rf := r.c, r.c.members, r.c.cfg.ReplicationFactor
	t0 := time.Now()
	chunks, err := c.repo.AllChunks(ctx)
	if err != nil {
		return api.RepairSummary{}, err
	}
	pendingRows, err := c.repo.PendingDeletes(ctx, nil, 1_000_000)
	if err != nil {
		return api.RepairSummary{}, err
	}
	pending := map[[2]string]bool{}
	for _, p := range pendingRows {
		pending[[2]string{p.ChunkID, p.WorkerID}] = true
	}
	load := m.LoadMap()
	alive := m.AliveIDs()
	s := api.RepairSummary{ChunksChecked: len(chunks)}
	var jobs []repairJob

	for _, ch := range chunks {
		k := r.classify(ch)
		if k.counted < rf {
			s.UnderReplicated++
			if len(k.good) == 0 {
				s.Unavailable++
				if !r.unavailableLogged[ch.ChunkID] {
					r.unavailableLogged[ch.ChunkID] = true
					slog.Error("CHUNK_UNAVAILABLE", "chunk_id", ch.ChunkID, "replicas", describeReplicas(m, ch.Replicas))
				}
				continue
			}
			delete(r.unavailableLogged, ch.ChunkID)
			need := rf - k.counted
			holding := map[string]bool{}
			for _, rep := range ch.Replicas {
				holding[rep.WorkerID] = true
			}
			var targets []string
			for _, rep := range k.badAlive {
				targets = append(targets, rep.WorkerID)
			}
			var fresh []string
			for _, w := range alive {
				if !holding[w] && !pending[[2]string{ch.ChunkID, w}] {
					fresh = append(fresh, w)
				}
			}
			targets = append(targets, LeastLoaded(fresh, load, ch.ChunkID)...)
			if len(targets) > need {
				targets = targets[:need]
			}
			if len(targets) == 0 {
				slog.Warn("REPAIR_NO_TARGET", "chunk_id", ch.ChunkID, "alive", len(alive))
			}
			var sources []string
			for _, rep := range k.good {
				sources = append(sources, rep.WorkerID)
			}
			sort.SliceStable(sources, func(i, j int) bool {
				ai, aj := m.Status(sources[i]) != Alive, m.Status(sources[j]) != Alive
				if ai != aj {
					return !ai
				}
				return load[sources[i]] < load[sources[j]]
			})
			var deadOld []string
			for _, rep := range k.deadOld {
				deadOld = append(deadOld, rep.WorkerID)
			}
			for _, t := range targets {
				load[t] += ch.Size
				jobs = append(jobs, repairJob{chunk: ch, sources: sources, target: t, deadOld: deadOld})
			}
		} else if len(k.good) > rf {
			extras := append([]store.Replica(nil), k.good...)
			sort.Slice(extras, func(i, j int) bool {
				li, lj := load[extras[i].WorkerID], load[extras[j].WorkerID]
				if li != lj {
					return li > lj
				}
				return extras[i].WorkerID > extras[j].WorkerID
			})
			for _, rep := range extras[:len(k.good)-rf] {
				if err := c.repo.DropReplica(ctx, ch.ChunkID, rep.WorkerID, true); err != nil {
					return s, err
				}
				load[rep.WorkerID] -= ch.Size
				s.Trimmed++
				slog.Info("REPLICA_TRIMMED", "chunk_id", ch.ChunkID, "worker_id", rep.WorkerID, "reason", "over-replicated")
			}
		}
		if len(k.good) >= rf {
			for _, rep := range k.badAlive {
				if err := c.repo.DropReplica(ctx, ch.ChunkID, rep.WorkerID, rep.State != store.Missing); err != nil {
					return s, err
				}
				s.Cleaned++
			}
			for _, rep := range k.deadOld {
				if _, err := c.repo.SetReplicaState(ctx, ch.ChunkID, rep.WorkerID, store.Stale, store.OK); err != nil {
					return s, err
				}
			}
		}
	}

	if len(jobs) > 0 {
		sem := make(chan struct{}, max(1, c.cfg.RepairParallelism))
		var wg sync.WaitGroup
		var okCount int
		var cmu sync.Mutex
		for _, j := range jobs {
			wg.Add(1)
			sem <- struct{}{}
			go func(j repairJob) {
				defer wg.Done()
				defer func() { <-sem }()
				if r.copyChunk(ctx, j) {
					cmu.Lock()
					okCount++
					cmu.Unlock()
				}
			}(j)
		}
		wg.Wait()
		s.RepairsAttempted, s.RepairsSucceeded = len(jobs), okCount
	}
	if s.RepairsAttempted > 0 || s.Trimmed > 0 || s.Cleaned > 0 {
		c.gc.Wake()
		slog.Info("REPAIR_PASS", "duration_ms", time.Since(t0).Milliseconds(), "chunks_checked", s.ChunksChecked,
			"under_replicated", s.UnderReplicated, "repairs_attempted", s.RepairsAttempted,
			"repairs_succeeded", s.RepairsSucceeded, "trimmed", s.Trimmed, "cleaned", s.Cleaned)
	}
	last := s
	last.At = unixNow()
	r.smu.Lock()
	r.lastSummary = &last
	r.smu.Unlock()
	return s, nil
}

func (r *Repairer) copyChunk(ctx context.Context, j repairJob) bool {
	c, m := r.c, r.c.members
	cid := j.chunk.ChunkID
	for _, src := range j.sources {
		data, err := c.workers.GetChunk(ctx, m.URL(src), cid)
		if errors.Is(err, ErrChunkMissing) {
			_, _ = c.repo.SetReplicaState(ctx, cid, src, store.Missing, store.OK)
			slog.Warn("REPLICA_MISSING", "chunk_id", cid, "worker_id", src, "source", "repair")
			continue
		} else if err != nil {
			m.MarkSuspect(src, "repair read failed")
			continue
		}
		if checksum.SHA256Hex(data) != j.chunk.Checksum {
			_, _ = c.repo.SetReplicaState(ctx, cid, src, store.Corrupt, store.OK)
			slog.Warn("REPLICA_CORRUPT", "chunk_id", cid, "worker_id", src, "source", "repair")
			continue
		}
		if err := c.workers.PutChunk(ctx, m.URL(j.target), cid, data, j.chunk.Checksum, j.chunk.ObjectID, j.chunk.Index); err != nil {
			slog.Warn("REPAIR_FAILED", "chunk_id", cid, "target", j.target, "error", err)
			return false
		}
		ok, err := c.repo.AddReplicaOK(ctx, cid, j.target)
		if err != nil || !ok {
			slog.Info("REPAIR_DISCARDED", "chunk_id", cid, "target", j.target, "reason", "object deleted")
			return false
		}
		for _, w := range j.deadOld {
			_, _ = c.repo.SetReplicaState(ctx, cid, w, store.Stale, store.OK)
		}
		replaced := strings.Join(j.deadOld, ",")
		if replaced == "" {
			replaced = "-"
		}
		slog.Info("REPAIR_DONE", "chunk_id", cid, "source", src, "target", j.target, "replaced", replaced)
		return true
	}
	slog.Error("REPAIR_FAILED", "chunk_id", cid, "target", j.target, "reason", "no valid source replica")
	return false
}

// Reconcile compares a worker's verified inventory with the metadata.
func (r *Repairer) Reconcile(ctx context.Context, workerID string) (*api.ReconcileResult, error) {
	c := r.c
	url := c.members.URL(workerID)
	if url == "" {
		return nil, nil
	}
	inv, err := c.workers.Inventory(ctx, url, workerID, true)
	if err != nil {
		slog.Warn("RECONCILE_FAILED", "worker_id", workerID, "error", err)
		return nil, err
	}
	onDisk := make(map[string]api.InventoryItem, len(inv.Chunks))
	for _, it := range inv.Chunks {
		onDisk[it.ChunkID] = it
	}
	rows, err := c.repo.WorkerReplicas(ctx, workerID)
	if err != nil {
		return nil, err
	}
	changes := map[string]string{}
	res := &api.ReconcileResult{}
	known := make(map[string]bool, len(rows))
	for _, row := range rows {
		known[row.ChunkID] = true
		it, present := onDisk[row.ChunkID]
		newState := store.OK
		switch {
		case !present:
			newState = store.Missing
			res.Missing++
		case it.Corrupt || it.Checksum != row.Checksum || it.Size != row.Size:
			newState = store.Corrupt
			res.Corrupt++
		default:
			res.OK++
			if row.State != store.OK {
				res.Restored++
			}
		}
		if newState != row.State {
			changes[row.ChunkID] = newState
			if newState != store.OK {
				slog.Warn("REPLICA_"+newState, "chunk_id", row.ChunkID, "worker_id", workerID, "source", "reconcile")
			}
		}
	}
	if err := c.repo.SetReplicaStates(ctx, workerID, changes); err != nil {
		return nil, err
	}
	res.OrphansDeleted = c.gc.DeleteOrphans(ctx, workerID, url, inv, known)
	slog.Info("RECONCILED", "worker_id", workerID, "on_disk", len(inv.Chunks), "expected", len(rows),
		"ok", res.OK, "restored", res.Restored, "missing", res.Missing, "corrupt", res.Corrupt,
		"orphans_deleted", res.OrphansDeleted)
	c.gc.Wake()
	r.Wake()
	return res, nil
}

// Report summarises replication health. A replica is live when it is OK on an ALIVE worker.
func (r *Repairer) Report(ctx context.Context, includeChunks bool, limit int) (api.ReplicationReport, error) {
	c, m, rf := r.c, r.c.members, r.c.cfg.ReplicationFactor
	chunks, err := c.repo.AllChunks(ctx)
	if err != nil {
		return api.ReplicationReport{}, err
	}
	out := api.ReplicationReport{ReplicationFactor: rf, TotalChunks: len(chunks)}
	var problems []api.ProblemChunk
	for _, ch := range chunks {
		live, corrupt := 0, false
		for _, rep := range ch.Replicas {
			if rep.State == store.OK && m.Status(rep.WorkerID) == Alive {
				live++
			}
			if rep.State == store.Corrupt {
				corrupt = true
			}
		}
		switch {
		case live == rf:
			out.Healthy++
		case live == 0:
			out.Unavailable++
		case live < rf:
			out.UnderReplicated++
		default:
			out.OverReplicated++
		}
		if corrupt {
			out.Corrupt++
		}
		if live != rf || corrupt {
			problems = append(problems, api.ProblemChunk{ChunkID: ch.ChunkID, ObjectID: ch.ObjectID, Index: ch.Index,
				LiveOKReplicas: live, Replicas: describeReplicas(m, ch.Replicas)})
		}
	}
	if includeChunks {
		if len(problems) > limit {
			problems = problems[:limit]
		}
		out.ProblemChunks = problems
	}
	if out.PendingDeletes, err = c.repo.PendingDeleteCount(ctx); err != nil {
		return out, err
	}
	r.smu.Lock()
	out.LastRepairPass = r.lastSummary
	r.smu.Unlock()
	return out, nil
}

func describeReplicas(m *Membership, reps []store.Replica) []api.ReplicaInfo {
	out := make([]api.ReplicaInfo, 0, len(reps))
	for _, rep := range reps {
		out = append(out, api.ReplicaInfo{WorkerID: rep.WorkerID, State: rep.State, WorkerStatus: m.Status(rep.WorkerID)})
	}
	return out
}
