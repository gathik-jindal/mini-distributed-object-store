# Design: Mini Distributed Cloud Object Store

## 1. Goals and scope

The system meets the brief ([Project-Instructions.pdf](Project-Instructions.pdf)): 1 controller, 4 storage workers, 1 MB chunks, replication factor 2, persistent worker storage, deployment with Docker Compose, the fixed public and internal APIs, worker-failure handling, and corruption handling.

The two design priorities were **correctness** and **observability**. Every mechanism is visible through an API endpoint, the `dosctl` CLI, or a structured log event, so each one can be shown in the demo.

## 2. Architecture

```
            ┌──────────── Client(s) ─────────────┐
            │  dosctl CLI (cobra) / curl          │
            └───────────────┬─────────────────────┘
                            │ HTTP :8000  /api/v1/*   (only public entry point)
                 ┌──────────▼───────────┐
                 │      CONTROLLER       │  Go net/http, single process
                 │ • public REST API     │  SQLite metadata (volume: controller-data)
                 │ • chunker + placement │  background goroutines:
                 │ • read path + verify  │   failure detector · repair/re-replication
                 │ • /internal register, │   reconciliation · GC
                 │   heartbeat           │
                 └──┬──────┬──────┬────┬─┘
        /internal/chunks/* (PUT/GET/DELETE), inventory     ▲ register + heartbeat
          ┌─────────┘      │      │    └─────────┐         │ (every 2s)
     ┌────▼───┐       ┌────▼───┐ ┌▼───────┐ ┌────▼───┐     │
     │worker1 │       │worker2 │ │worker3 │ │worker4 │─────┘
     │ :9000  │       │ :9000  │ │ :9000  │ │ :9000  │  chunk files + checksum sidecars,
     │vol: w1 │       │vol: w2 │ │vol: w3 │ │vol: w4 │  local scrubber
     └────────┘       └────────┘ └────────┘ └────────┘
```

| Decision | Rationale |
|---|---|
| The controller is the only metadata authority and the only public endpoint | Clients never learn where chunks live (spec §5.2). Worker ports are not published |
| Objects are **immutable** (write-once); only custom metadata changes | A new upload gets a new Object ID. This removes most consistency problems: there are no partial overwrites and no version conflicts between replicas |
| The controller is in the data path (it streams chunks to and from workers) | Simple to build and to reason about at this scale. Trade-off: controller bandwidth limits throughput (§9) |
| Single controller | Allowed by the spec. Its metadata is on a persistent volume and the controller recovers fully when restarted |
| One Docker image with three static binaries | `controller`, `worker`, `dosctl`, built from a multi-stage Dockerfile onto Alpine |

**Tech stack:**

- Go 1.23 with only the standard library for HTTP (Go 1.22+ `ServeMux` method and path patterns) and logging (`log/slog`, key=value).
- `modernc.org/sqlite`: a pure-Go SQLite, so no cgo and fully static binaries.
- `spf13/cobra` for the CLI.
- `httptest`-based in-process cluster tests, plus integration tests behind a build tag.

### Code layout

| Package | Responsibility |
|---|---|
| `cmd/controller`, `cmd/worker` | Wiring, `signal.NotifyContext`, graceful shutdown |
| `cmd/dosctl` | Cobra CLI on top of `internal/client` |
| `internal/api` | Shared DTOs: RegisterRequest, Heartbeat, ObjectMeta, ChunkInfo, ErrorEnvelope, … |
| `internal/config` | Env parsing with defaults |
| `internal/httpx` | JSON helpers, the error envelope, and the `APIError` type (maps to an HTTP status) |
| `internal/checksum` | sha256 helpers |
| `internal/controller` | `server.go` routes · `objects.go` upload/download/metadata/list/delete · `internal.go` register/heartbeat · `cluster.go` admin · `placement.go` HRW + least-loaded · `workerclient.go` · `membership.go` · `repair.go` · `gc.go` |
| `internal/controller/store` | Embedded `schema.sql`, SQLite setup, every metadata query |
| `internal/worker` | `chunkstore.go` · `server.go` · `agent.go` register + heartbeat · `scrubber.go` |
| `internal/client` | Go SDK for the public API |

## 3. Data model

**Controller (SQLite, WAL mode, schema embedded with `go:embed`):**

| Table | Contents |
|---|---|
| `objects` | `object_id` (UUIDv4), `name`, `size`, `content_type`, `checksum` (sha256 of the whole object), `chunk_size`, `chunk_count`, `custom_metadata` (JSON), `status` (UPLOADING / COMMITTED / DELETING), `created_at`, `updated_at` |
| `chunks` | `chunk_id` = `{object_id}-c{index:05d}` (unique and order-preserving), `object_id`, `idx`, `size`, `checksum` (sha256) |
| `replicas` | (`chunk_id`, `worker_id`) → `state` ∈ OK / CORRUPT / MISSING / STALE |
| `workers` | `url`, `status`, `last_heartbeat`, `used_bytes`, `free_bytes`, `chunk_count` |
| `pending_deletes` | (`chunk_id`, `worker_id`, `attempts`): deletes retried until the worker confirms |

The pool is limited to one connection. SQLite has a single writer anyway, and serialising in-process avoids `database is locked` errors.

**Worker (files on its own volume):**

- `/data/chunks/<ab>/<chunk_id>.bin`: the chunk bytes. `<ab>` is the first two hex characters of `sha1(chunk_id)`, which keeps directories small.
- `/data/chunks/<ab>/<chunk_id>.json`: a sidecar holding `{checksum, size, object_id, index, stored_at}`.
- `/data/worker_id`: the worker's persisted identity.

`GET /api/v1/objects/{id}/metadata` returns every system field (`name`, `size`, `content_type`, `created_at`, `last_modified`, `checksum`, `chunk_count`), the `custom_metadata`, and a `chunks[]` list. Each chunk entry gives its id, index, size, checksum, and **replica locations with state and worker status**.

## 4. Write path (upload)

1. Validate the request. It must be `multipart/form-data` with a `file` part. `metadata`, if present, must be a JSON object with scalar values. Anything else returns **400**.
2. Require at least RF ALIVE workers, otherwise return **503 INSUFFICIENT_WORKERS**.
3. Insert the object row with status `UPLOADING`.
4. Read the upload **1 MiB at a time** (`io.ReadFull`; `net/http` spools large parts to disk), so the whole file is never held in memory. For each chunk:
   - compute its sha256 and feed the bytes into the whole-object hash;
   - **Placement:** rank the ALIVE workers by *rendezvous hashing* (`score = H(chunk_id | worker_id)`) and take the top 2. This is deterministic, spreads evenly, always picks distinct workers, and moves minimal data when membership changes;
   - `PUT` to both workers in parallel goroutines with the header `X-Chunk-Checksum`. The worker re-hashes, rejects a mismatch, then writes *temp file → fsync → rename* (atomic, so there are no torn chunks);
   - if a write fails, fall through to the next-ranked worker and mark the failed one SUSPECT;
   - a semaphore keeps up to 4 chunks in flight, and the first error cancels the rest through a context.
5. **Commit in one transaction:** insert the chunks and replicas (OK) and set the object to `COMMITTED`. The response is 201 with a `Location` header and the per-chunk placement.
6. **On abort**, delete the copies written so far (any leftovers are garbage-collected), drop the row, and return 503/500.

## 5. Read path (download)

1. Return 404 unless the object is `COMMITTED`.
2. **Pre-flight check:** every chunk needs an OK replica on a reachable worker, otherwise return 503. If every replica of a chunk is CORRUPT, return 500.
3. Fetch and verify chunk 0 **before** sending headers, so a total failure still produces a clean JSON error.
4. Stream the chunks in index order, prefetching the next chunk in a goroutine. The response carries the original `Content-Type`, `Content-Length`, `ETag` (the sha256), and `Content-Disposition`.
5. **Per-chunk verification and failover:** every fetched chunk is re-hashed against the controller's checksum. On a mismatch the replica is marked CORRUPT, repair is triggered (*read-repair*), and the next replica is tried. **Corrupted bytes are never sent.** An unreachable worker is marked SUSPECT and skipped; a 404 marks the replica MISSING.
6. If every copy of a *later* chunk fails mid-stream, the handler aborts the connection (`http.ErrAbortHandler`). The client sees a short body, which `dosctl` reports as truncated. This is an inherent limitation of streaming.

## 6. Membership and failure detection

Workers register on startup, retrying with exponential backoff, then heartbeat every 2 s with their disk stats and any chunks their scrubber found corrupt. The controller keeps the registry in memory behind a mutex and persists it to the `workers` table.

| State | Condition | Effect |
|---|---|---|
| ALIVE | Heartbeat within 5 s | Used for placement and reads |
| SUSPECT | No heartbeat for > 5 s, **or** a request to it just failed | No new placements; still tried for reads |
| DEAD | No heartbeat for > 10 s | Its replicas stop counting; after `REPAIR_GRACE` = 20 s its chunks are re-replicated |

If a heartbeat arrives from an unknown worker (for example after the controller's DB was wiped), the controller answers 404 and the worker re-registers. After a controller restart, every known worker starts as SUSPECT and is promoted by its next heartbeat.

## 7. Replication maintenance

A replica **counts** toward RF when its state is OK and its worker is ALIVE or SUSPECT, or when the worker is DEAD but still inside the grace period. The repair loop runs every 5 s, and immediately whenever a worker changes state or a corruption is found. For each chunk:

| Situation | Action |
|---|---|
| Counted < RF | Copy from a **checksum-verified** good replica: first overwrite a bad copy (CORRUPT/MISSING) in place on an ALIVE worker, otherwise use the *least-loaded* ALIVE worker not holding the chunk. Replicas on workers dead past the grace period become **STALE** |
| Worker back **within** grace | Reconciliation confirms its copies are OK. No data moves |
| Worker back **after** re-replication | Reconciliation verifies its STALE copies and marks them OK. Those chunks now have 3 copies, so the **trim** step deletes the extra copy from the most-loaded worker, keeping storage balanced |
| Counted > RF | Trim |
| No good copy left | Logged as `CHUNK_UNAVAILABLE` and reported. Repaired automatically if a holder comes back |

**Reconciliation** runs whenever a worker (re)registers or returns from DEAD. The controller fetches the worker's **verified inventory** (`GET /internal/workers/{id}/chunks?verify=true`, which re-hashes every chunk) and compares it with the DB:

- checksum matches → OK;
- absent → MISSING (then repaired);
- checksum differs → CORRUPT (then repaired);
- **orphans** (files the DB doesn't know about, e.g. objects deleted while the worker was down) older than 60 s → deleted.

Queued deletes for that worker are then flushed. Until reconciliation finishes, the repair loop neither trusts nor cleans up that worker's replica rows. This prevents a race in which a returning worker's valid copies would be discarded.

**Deletes** are tombstoned. The object's metadata is removed in one transaction, and every replica goes into `pending_deletes`. A GC loop then deletes the chunks on workers, retrying until each worker confirms. A worker that is down receives its deletes when it returns, and orphan sweeps act as a backstop.

## 8. Data integrity

Integrity is checked at four layers:

1. **On write:** the worker verifies `X-Chunk-Checksum` before committing (400 on mismatch), and the atomic rename prevents torn writes.
2. **On read:** the controller verifies every chunk against its own authoritative sha256, fails over to another replica, and repairs the bad one.
3. **Background scrub:** each worker re-hashes all of its chunks every 60 s, and immediately on `POST /internal/scrub`. Corrupt chunk IDs are sent in heartbeats, and the controller marks them CORRUPT and repairs them. This **detects corruption that no read has touched**.
4. **End to end:** the whole-object sha256 is exposed as the `ETag` and in the metadata, and `dosctl download` recomputes it.

The controller's checksum is authoritative. The worker's sidecar only drives local scrubbing, so corrupting the data file and the sidecar together is still caught by the controller on read or reconciliation.

## 9. Trade-offs and limitations

- **The controller is a single point of failure and a throughput bottleneck.** Production systems (GFS, HDFS) have clients stream directly to data nodes and replicate the master. We chose simplicity, as the spec allows.
- **Failure detection uses timeouts.** A slow but alive worker can be marked SUSPECT; the next heartbeat clears it. The grace period stops a short restart from triggering a full re-replication.
- **Mid-stream failure** of every replica of a later chunk can only be signalled by a truncated response.
- **SQLite** gives one writer, and a single connection serialises metadata access. That is enough for one controller, because metadata operations are tiny compared with chunk I/O.
- With RF = 2, losing two specific workers at once can make some chunks unavailable until one of them returns. Data on disk survives because the volumes persist.

## 10. Testing

- **Unit:** chunk store (atomicity, checksum rejection, idempotency, corrupt-overwrite, id validation), placement (distinct targets, even spread, minimal movement).
- **In-process cluster** (`internal/controller/cluster_test.go`): a real controller and 4 real workers on `httptest` servers. Any worker can be made to drop connections like a stopped container. Covered: round-trips at 0 B, 1 B, exactly 1 chunk, 1 chunk + 1 B, and 5.5 chunks; metadata and pagination; deletes (including while a worker is down); upload failover; 503 with too few workers; dead-worker re-replication and return with trimming; return within grace; double failure; read-repair; all-copies-corrupt returning 500; scrubber via the API and via heartbeat; deleted chunk files; orphans; controller restart. Clean under `-race`.
- **Live stack** (`tests/integration`, `//go:build integration`): the same scenarios against Docker Compose, including a real `docker stop` / `docker start worker2`.
