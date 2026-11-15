-- Controller metadata (SQLite, WAL mode).

CREATE TABLE IF NOT EXISTS objects (
    object_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    size            INTEGER NOT NULL DEFAULT 0,
    content_type    TEXT NOT NULL,
    checksum        TEXT,
    chunk_size      INTEGER NOT NULL,
    chunk_count     INTEGER NOT NULL DEFAULT 0,
    custom_metadata TEXT NOT NULL DEFAULT '{}',   -- JSON object
    status          TEXT NOT NULL,                -- UPLOADING | COMMITTED | DELETING
    created_at      TEXT NOT NULL,                -- UTC ISO-8601
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_objects_status_created ON objects(status, created_at);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id  TEXT PRIMARY KEY,                   -- {object_id}-c{index:05d}
    object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
    idx       INTEGER NOT NULL,
    size      INTEGER NOT NULL,
    checksum  TEXT NOT NULL                       -- sha256 hex
);
CREATE INDEX IF NOT EXISTS ix_chunks_object ON chunks(object_id, idx);

CREATE TABLE IF NOT EXISTS replicas (
    chunk_id   TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
    worker_id  TEXT NOT NULL,
    state      TEXT NOT NULL,                     -- OK | CORRUPT | MISSING | STALE
    updated_at TEXT NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX IF NOT EXISTS ix_replicas_worker ON replicas(worker_id);

CREATE TABLE IF NOT EXISTS workers (
    worker_id      TEXT PRIMARY KEY,
    url            TEXT NOT NULL,
    status         TEXT NOT NULL,                 -- ALIVE | SUSPECT | DEAD
    last_heartbeat REAL,
    registered_at  REAL,
    used_bytes     INTEGER NOT NULL DEFAULT 0,
    free_bytes     INTEGER NOT NULL DEFAULT 0,
    chunk_count    INTEGER NOT NULL DEFAULT 0
);

-- chunk copies that must be deleted from a worker; retried until confirmed
CREATE TABLE IF NOT EXISTS pending_deletes (
    chunk_id   TEXT NOT NULL,
    worker_id  TEXT NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);
