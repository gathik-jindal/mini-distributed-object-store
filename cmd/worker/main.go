// Command worker runs a storage worker: chunk storage on its persistent volume,
// the internal chunk API, heartbeats to the controller and the scrubber.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/config"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/worker"
)

func main() {
	cfg := config.Load()
	if os.Getenv("PORT") == "" {
		cfg.Port = 9000
	}
	httpx.SetupLogging(cfg.LogLevel, "worker:"+cfg.WorkerID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cs, err := worker.NewChunkStore(cfg.DataDir)
	if err != nil {
		slog.Error("open chunk store", "error", err)
		os.Exit(1)
	}
	id, err := worker.LoadOrInitWorkerID(cfg.DataDir, cfg.WorkerID)
	if err != nil {
		slog.Error("worker id", "error", err)
		os.Exit(1)
	}
	slog.Info("WORKER_START", "worker_id", id, "data_dir", cfg.DataDir)

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           (&worker.Server{WorkerID: id, Store: cs}).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			stop()
		}
	}()

	agent := &worker.Agent{WorkerID: id, WorkerURL: cfg.WorkerURL, ControllerURL: cfg.ControllerURL,
		Interval: cfg.HeartbeatInterval, Store: cs, HTTP: &http.Client{Timeout: cfg.HTTPTimeout}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); agent.Run(ctx) }()
	go func() { defer wg.Done(); worker.ScrubLoop(ctx, id, cs, cfg.ScrubInterval) }()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	wg.Wait()
}
