// Package api holds the request/response types shared by the controller,
// the workers, the Go client SDK and dosctl.
package api

// ---------------------------------------------------------------- errors

// ErrorEnvelope is the body of every error response:
// {"error": {"code": "OBJECT_NOT_FOUND", "message": "...", "details": {...}}}
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

// ---------------------------------------------------------------- internal: controller <-> worker

type WorkerStats struct {
	UsedBytes  int64 `json:"used_bytes"`
	FreeBytes  int64 `json:"free_bytes"`
	ChunkCount int   `json:"chunk_count"`
}

type RegisterRequest struct {
	WorkerID string `json:"worker_id"`
	URL      string `json:"url"`
	WorkerStats
}

type RegisterResponse struct {
	Status            string  `json:"status"`
	WorkerID          string  `json:"worker_id"`
	HeartbeatInterval float64 `json:"heartbeat_interval"`
}

type Heartbeat struct {
	WorkerID      string      `json:"worker_id"`
	Stats         WorkerStats `json:"stats"`
	CorruptChunks []string    `json:"corrupt_chunks"`
}

type InventoryItem struct {
	ChunkID  string  `json:"chunk_id"`
	Size     int64   `json:"size"`
	Checksum string  `json:"checksum"`
	StoredAt float64 `json:"stored_at"` // unix seconds
	Corrupt  bool    `json:"corrupt"`   // only meaningful when verified
}

type Inventory struct {
	WorkerID string          `json:"worker_id"`
	Verified bool            `json:"verified"`
	Chunks   []InventoryItem `json:"chunks"`
}

type ScrubResult struct {
	WorkerID   string   `json:"worker_id"`
	Scanned    int      `json:"scanned"`
	Corrupt    []string `json:"corrupt"`
	DurationMS int64    `json:"duration_ms"`
}

type PutChunkResponse struct {
	ChunkID  string `json:"chunk_id"`
	Size     int    `json:"size"`
	Checksum string `json:"checksum"`
	Created  bool   `json:"created"`
}

// ExpectedChunk is one row of the controller's view of a worker's chunks.
type ExpectedChunk struct {
	ChunkID  string `json:"chunk_id"`
	State    string `json:"state"`
	ObjectID string `json:"object_id"`
	Index    int    `json:"index"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

type ExpectedChunks struct {
	WorkerID string          `json:"worker_id"`
	Status   string          `json:"status"`
	Count    int             `json:"count"`
	Chunks   []ExpectedChunk `json:"chunks"`
}

// ---------------------------------------------------------------- public objects API

type ReplicaInfo struct {
	WorkerID     string `json:"worker_id"`
	State        string `json:"state"`
	WorkerStatus string `json:"worker_status,omitempty"`
}

type ChunkInfo struct {
	ChunkID  string        `json:"chunk_id"`
	Index    int           `json:"index"`
	Size     int64         `json:"size"`
	Checksum string        `json:"checksum"`
	Replicas []ReplicaInfo `json:"replicas"`
}

// ObjectMeta is returned by GET /api/v1/objects/{id}/metadata.
type ObjectMeta struct {
	ObjectID          string         `json:"object_id"`
	Name              string         `json:"name"`
	Size              int64          `json:"size"`
	ContentType       string         `json:"content_type"`
	Checksum          string         `json:"checksum"`
	ChecksumAlgorithm string         `json:"checksum_algorithm"`
	Status            string         `json:"status"`
	ChunkSize         int            `json:"chunk_size"`
	ChunkCount        int            `json:"chunk_count"`
	ReplicationFactor int            `json:"replication_factor"`
	CreatedAt         string         `json:"created_at"`
	UpdatedAt         string         `json:"updated_at"`
	LastModified      string         `json:"last_modified"`
	CustomMetadata    map[string]any `json:"custom_metadata"`
	Chunks            []ChunkInfo    `json:"chunks"`
}

type ChunkPlacement struct {
	ChunkID  string   `json:"chunk_id"`
	Index    int      `json:"index"`
	Size     int64    `json:"size"`
	Checksum string   `json:"checksum"`
	Workers  []string `json:"workers"`
}

// UploadResponse is returned by POST /api/v1/objects (201).
type UploadResponse struct {
	ObjectID       string           `json:"object_id"`
	Name           string           `json:"name"`
	Size           int64            `json:"size"`
	ContentType    string           `json:"content_type"`
	Checksum       string           `json:"checksum"`
	ChunkSize      int              `json:"chunk_size"`
	ChunkCount     int              `json:"chunk_count"`
	Status         string           `json:"status"`
	CreatedAt      string           `json:"created_at"`
	LastModified   string           `json:"last_modified"`
	CustomMetadata map[string]any   `json:"custom_metadata"`
	Chunks         []ChunkPlacement `json:"chunks"`
}

type ObjectSummary struct {
	ObjectID     string `json:"object_id"`
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	ContentType  string `json:"content_type"`
	Checksum     string `json:"checksum"`
	ChunkCount   int    `json:"chunk_count"`
	CreatedAt    string `json:"created_at"`
	LastModified string `json:"last_modified"`
}

type ListResponse struct {
	Items  []ObjectSummary `json:"items"`
	Total  int             `json:"total"`
	Limit  int             `json:"limit"`
	Offset int             `json:"offset"`
}

// ---------------------------------------------------------------- cluster / admin

type WorkerInfo struct {
	WorkerID              string   `json:"worker_id"`
	URL                   string   `json:"url"`
	Status                string   `json:"status"`
	LastHeartbeat         float64  `json:"last_heartbeat"`
	SecondsSinceHeartbeat *float64 `json:"seconds_since_heartbeat"`
	UsedBytes             int64    `json:"used_bytes"`
	FreeBytes             int64    `json:"free_bytes"`
	ChunkCount            int      `json:"chunk_count"`
	ReplicasOK            int      `json:"replicas_ok"`
}

type WorkersResponse struct {
	Workers           []WorkerInfo `json:"workers"`
	Alive             int          `json:"alive"`
	ReplicationFactor int          `json:"replication_factor"`
}

type ProblemChunk struct {
	ChunkID        string        `json:"chunk_id"`
	ObjectID       string        `json:"object_id"`
	Index          int           `json:"index"`
	LiveOKReplicas int           `json:"live_ok_replicas"`
	Replicas       []ReplicaInfo `json:"replicas"`
}

type RepairSummary struct {
	ChunksChecked    int     `json:"chunks_checked"`
	UnderReplicated  int     `json:"under_replicated"`
	Unavailable      int     `json:"unavailable"`
	RepairsAttempted int     `json:"repairs_attempted"`
	RepairsSucceeded int     `json:"repairs_succeeded"`
	Trimmed          int     `json:"trimmed"`
	Cleaned          int     `json:"cleaned"`
	At               float64 `json:"at,omitempty"`
}

type ReplicationReport struct {
	ReplicationFactor int            `json:"replication_factor"`
	TotalChunks       int            `json:"total_chunks"`
	Healthy           int            `json:"healthy"`
	UnderReplicated   int            `json:"under_replicated"`
	OverReplicated    int            `json:"over_replicated"`
	Corrupt           int            `json:"corrupt"`
	Unavailable       int            `json:"unavailable"`
	PendingDeletes    int            `json:"pending_deletes"`
	ProblemChunks     []ProblemChunk `json:"problem_chunks,omitempty"`
	LastRepairPass    *RepairSummary `json:"last_repair_pass,omitempty"`
}

type RepairResponse struct {
	Summary RepairSummary     `json:"summary"`
	Report  ReplicationReport `json:"report"`
}

type ScrubWorkerResult struct {
	ScrubResult
	Error string `json:"error,omitempty"`
}

type ScrubResponse struct {
	Workers      map[string]ScrubWorkerResult `json:"workers"`
	NewlyFlagged int                          `json:"newly_flagged"`
	Repair       *RepairSummary               `json:"repair"`
}

type ReconcileResult struct {
	OK             int `json:"ok"`
	Restored       int `json:"restored"`
	Missing        int `json:"missing"`
	Corrupt        int `json:"corrupt"`
	OrphansDeleted int `json:"orphans_deleted"`
}

type ReconcileResponse struct {
	WorkerID string           `json:"worker_id"`
	Result   *ReconcileResult `json:"result"`
}
