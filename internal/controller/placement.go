package controller

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// Replica placement.
//
// New chunks use rendezvous (highest-random-weight) hashing: every worker gets
// a score H(chunk_id | worker_id) and the chunk goes to the top-RF workers.
// Deterministic, evenly spread, always distinct workers, and minimal data
// movement when membership changes.
//
// Repair targets use the least-loaded eligible worker, filling the emptiest first.

func hrwScore(chunkID, workerID string) uint64 {
	sum := sha256.Sum256([]byte(chunkID + "|" + workerID))
	return binary.BigEndian.Uint64(sum[:8])
}

// RankWorkers returns the candidate workers for a chunk, best first.
func RankWorkers(chunkID string, workerIDs []string) []string {
	out := dedupe(workerIDs)
	sort.Slice(out, func(i, j int) bool {
		si, sj := hrwScore(chunkID, out[i]), hrwScore(chunkID, out[j])
		if si != sj {
			return si > sj
		}
		return out[i] > out[j]
	})
	return out
}

// LeastLoaded orders candidates by ascending load; the HRW score breaks ties so
// equally loaded workers don't always lose to the same one.
func LeastLoaded(candidates []string, load map[string]int64, chunkID string) []string {
	out := dedupe(candidates)
	sort.Slice(out, func(i, j int) bool {
		li, lj := load[out[i]], load[out[j]]
		if li != lj {
			return li < lj
		}
		si, sj := hrwScore(chunkID, out[i]), hrwScore(chunkID, out[j])
		if si != sj {
			return si > sj
		}
		return out[i] < out[j]
	})
	return out
}

func dedupe(ids []string) []string {
	seen := make(map[string]bool, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}
