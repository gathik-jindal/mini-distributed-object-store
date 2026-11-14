// Package config reads every tunable from environment variables, with defaults.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// data layout
	ChunkSize         int
	ReplicationFactor int

	// membership / failure detection
	HeartbeatInterval time.Duration
	SuspectAfter      time.Duration
	DeadAfter         time.Duration
	DetectorInterval  time.Duration

	// maintenance
	RepairGrace         time.Duration
	RepairInterval      time.Duration
	GCInterval          time.Duration
	OrphanSweepInterval time.Duration
	OrphanMinAge        time.Duration
	ScrubInterval       time.Duration

	// concurrency
	UploadParallelism int
	RepairParallelism int
	HTTPTimeout       time.Duration

	// addressing
	ControllerURL string
	WorkerID      string
	WorkerURL     string
	Host          string
	Port          int

	// storage
	DataDir string
	DBPath  string

	LogLevel string
}

// Defaults returns the configuration used when no environment variable is set.
func Defaults() Config {
	return Config{
		ChunkSize:           1 << 20,
		ReplicationFactor:   2,
		HeartbeatInterval:   2 * time.Second,
		SuspectAfter:        5 * time.Second,
		DeadAfter:           10 * time.Second,
		DetectorInterval:    time.Second,
		RepairGrace:         20 * time.Second,
		RepairInterval:      5 * time.Second,
		GCInterval:          5 * time.Second,
		OrphanSweepInterval: 60 * time.Second,
		OrphanMinAge:        60 * time.Second,
		ScrubInterval:       60 * time.Second,
		UploadParallelism:   4,
		RepairParallelism:   4,
		HTTPTimeout:         10 * time.Second,
		ControllerURL:       "http://controller:8000",
		WorkerID:            "worker1",
		WorkerURL:           "http://worker1:9000",
		Host:                "0.0.0.0",
		Port:                8000,
		DataDir:             "/data",
		DBPath:              "/data/meta.db",
		LogLevel:            "INFO",
	}
}

// Load applies environment overrides on top of Defaults.
func Load() Config {
	c := Defaults()
	intVar(&c.ChunkSize, "CHUNK_SIZE")
	intVar(&c.ReplicationFactor, "REPLICATION_FACTOR")
	durVar(&c.HeartbeatInterval, "HEARTBEAT_INTERVAL")
	durVar(&c.SuspectAfter, "SUSPECT_AFTER")
	durVar(&c.DeadAfter, "DEAD_AFTER")
	durVar(&c.DetectorInterval, "DETECTOR_INTERVAL")
	durVar(&c.RepairGrace, "REPAIR_GRACE")
	durVar(&c.RepairInterval, "REPAIR_INTERVAL")
	durVar(&c.GCInterval, "GC_INTERVAL")
	durVar(&c.OrphanSweepInterval, "ORPHAN_SWEEP_INTERVAL")
	durVar(&c.OrphanMinAge, "ORPHAN_MIN_AGE")
	durVar(&c.ScrubInterval, "SCRUB_INTERVAL")
	intVar(&c.UploadParallelism, "UPLOAD_PARALLELISM")
	intVar(&c.RepairParallelism, "REPAIR_PARALLELISM")
	durVar(&c.HTTPTimeout, "HTTP_TIMEOUT")
	strVar(&c.ControllerURL, "CONTROLLER_URL")
	strVar(&c.WorkerID, "WORKER_ID")
	strVar(&c.WorkerURL, "WORKER_URL")
	strVar(&c.Host, "HOST")
	intVar(&c.Port, "PORT")
	strVar(&c.DataDir, "DATA_DIR")
	strVar(&c.DBPath, "DB_PATH")
	strVar(&c.LogLevel, "LOG_LEVEL")
	return c
}

func strVar(dst *string, key string) {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		*dst = v
	}
}

func intVar(dst *int, key string) {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			*dst = n
		}
	}
}

// durVar accepts plain seconds ("2", "0.5") or Go durations ("2s", "500ms").
func durVar(dst *time.Duration, key string) {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return
	}
	if d, err := ParseDuration(v); err == nil {
		*dst = d
	}
}

// ParseDuration parses seconds as a number or a Go duration string.
func ParseDuration(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return time.ParseDuration(v)
}
