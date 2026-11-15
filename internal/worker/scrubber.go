package worker

import (
	"context"
	"log/slog"
	"time"
)

// ScrubLoop periodically re-hashes every local chunk. Corrupt chunks are
// remembered by the store and reported to the controller in heartbeats.
func ScrubLoop(ctx context.Context, workerID string, store *ChunkStore, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := RunScrub(workerID, store); err != nil {
				slog.Error("scrub failed", "error", err)
			}
		}
	}
}
