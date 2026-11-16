// Command controller runs the object store controller: the public REST API,
// the internal worker API and the background maintenance loops.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/config"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/controller"
	"github.com/gathik-jindal/mini-distributed-object-store/internal/httpx"
)

func main() {
	cfg := config.Load()
	httpx.SetupLogging(cfg.LogLevel, "controller")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctl, err := controller.New(ctx, cfg)
	if err != nil {
		slog.Error("startup failed", "error", err)
		os.Exit(1)
	}
	ctl.Start(ctx)

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Handler:           ctl.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		slog.Info("listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	_ = ctl.Close()
}
