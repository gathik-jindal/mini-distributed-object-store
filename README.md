# Mini Distributed Cloud Object Store

An S3-style object store written in Go. It runs as **1 controller + 4 storage workers + 1 or more clients** under Docker Compose.

Each object is split into **1 MiB chunks**. Every chunk is stored on **2 different workers**, and each worker writes to its own persistent volume. The controller:

- detects failed workers and missing or corrupted copies;
- restores replication automatically;
- never returns corrupted data.

The assignment brief is in [docs/Project-Instructions.pdf](docs/Project-Instructions.pdf), and the architecture write-up is in [docs/DESIGN.md](docs/DESIGN.md).

```
client(s) ──HTTP :8000──▶ controller ──/internal/chunks/*──▶ worker1..4  (one volume each)
 dosctl / curl            Go + SQLite               ◀── register + heartbeat (2s)
```

## Requirements

| Tool | Needed for |
|---|---|
| Docker + the Compose plugin (`docker compose version`) | Running the system (required) |
| Go ≥ 1.23 | Building `dosctl` on the host and running the tests (optional: the client also runs in a container) |
| GNU make | The shortcuts below (optional: every target is a plain `go` / `docker compose` command) |
| curl, python3 | Used by `scripts/demo.sh` and `scripts/gen_samples.sh` |

## Build & start

```bash
make up          # = docker compose up -d --build   (controller + worker1..4)
make ps          # all 5 containers should become "healthy"
make build       # host binaries in ./bin: controller, worker, dosctl
```

The API is at <http://localhost:8000>. Worker ports are not published, so clients only ever talk to the controller.

To stop the stack:

```bash
make down        # stop, keep data
make reset       # stop and wipe all volumes (fresh cluster)
```

## Using the client (`dosctl`)

```bash
# (a) host binary
make build && export DOS_URL=http://localhost:8000
./bin/dosctl --help

# (b) inside the client container (nothing to install). ./samples is mounted at /work
docker compose run --rm client cluster
docker compose run --rm client upload video-20MB.mp4 -m owner=alice
```

| Command | What it does |
|---|---|
| `dosctl upload FILE [-m k=v ...] [--meta-json '{...}']` | Upload; prints the Object ID and which workers hold each chunk |
| `dosctl download ID [-o FILE]` | Download and verify the sha256 end to end (`MATCH`/`MISMATCH`) |
| `dosctl stat ID` | System and custom metadata, plus every chunk with its replica locations and states |
| `dosctl ls [--limit N --offset M]` | List objects |
| `dosctl meta get ID` / `meta set ID k=v ...` / `meta rm ID k ...` | Read or modify custom metadata (no re-upload) |
| `dosctl rm ID ...` | Delete objects |
| `dosctl cluster` | Worker status (ALIVE/SUSPECT/DEAD), last heartbeat, chunk counts |
| `dosctl replication` | Replication health: healthy / under-replicated / corrupt / unavailable chunks |
| `dosctl scrub` | Make every worker re-hash its chunks now; corrupt copies get repaired |
| `dosctl repair` | Force a repair pass now (it also runs every 5 s on its own) |

Add `--json` to any command for raw JSON output.

## REST API

### Public API (spec §5)

| Method | Endpoint | Success | Errors |
|---|---|---|---|
| POST | `/api/v1/objects` (multipart: `file`, optional `metadata` JSON) | 201 + `Location` | 400, 503 |
| GET | `/api/v1/objects/{id}` (binary, original `Content-Type`, `ETag` = sha256) | 200 | 404, 500 (all copies corrupt), 503 |
| GET | `/api/v1/objects/{id}/metadata` | 200 | 404 |
| PATCH | `/api/v1/objects/{id}/metadata` (JSON merge-patch; `null` removes a key) | 200 | 400, 404, 409 |
| DELETE | `/api/v1/objects/{id}` | 204 | 404, 409 |
| GET | `/api/v1/objects?limit=50&offset=0` | 200 | 400 |

Every error response has the same JSON shape:

```json
{"error": {"code": "OBJECT_NOT_FOUND", "message": "object 123 not found", "details": {}}}
```

Example session with curl:

```bash
curl -F file=@samples/gradient.png -F 'metadata={"owner":"alice"}' localhost:8000/api/v1/objects
curl localhost:8000/api/v1/objects/<id> -o out.png
curl localhost:8000/api/v1/objects/<id>/metadata
curl -X PATCH -H 'Content-Type: application/json' -d '{"owner":null,"tag":"v2"}' localhost:8000/api/v1/objects/<id>/metadata
curl -X DELETE localhost:8000/api/v1/objects/<id>
```

### Internal API: controller ↔ workers (spec §6)

`POST /internal/workers/register`, `POST /internal/workers/heartbeat`, `PUT|GET|DELETE /internal/chunks/{chunk_id}` (on workers), and `GET /internal/workers/{worker_id}/chunks`. On a worker, that last endpoint returns the chunks the worker actually holds (`?verify=true` re-hashes them). On the controller, it returns the chunks the worker *should* hold.

### Cluster endpoints (for observation and demos)

`GET /api/v1/cluster/workers`, `GET /api/v1/cluster/replication`, `POST /api/v1/cluster/repair`, `POST /api/v1/cluster/scrub`, and `POST /api/v1/cluster/workers/{id}/reconcile`.

## Demonstration

```bash
make samples                 # creates ./samples (text, png, 5.5 MiB, 20 MiB)
make demo                    # guided walkthrough of all 6 required demos (pauses between steps)
AUTO=1 scripts/demo.sh       # run straight through
scripts/demo.sh 5            # only demo #5 (worker failure)
```

| # | Demo | What you'll see |
|---|---|---|
| 1 | Object storage | Upload a text file and a PNG, download them, `cmp` against the originals, sha256 `MATCH` |
| 2 | Chunking & distribution | A 5.5 MiB file becomes 6 chunks (5 × 1 MiB + 512 KiB) spread over all workers; chunk files counted on each volume |
| 3 | Replication | Every chunk has 2 copies on 2 different workers; the replication report shows all chunks healthy |
| 4 | Metadata | Upload with custom metadata, then get, set and remove keys; `last_modified` changes while the chunk files are untouched |
| 5 | Worker failure | `docker stop worker2`: SUSPECT after 5 s, DEAD after 10 s. Downloads keep working. After a 20 s grace period, lost copies are re-replicated. `docker start worker2`: its old copies are verified, extra copies are trimmed, and every chunk is back to exactly 2 |
| 6 | Data integrity | `scripts/corrupt_chunk.sh` flips bytes on disk. (a) A read detects the bad copy, serves a good one, and rewrites the bad one. (b) The scrubber finds corruption without any read. (c) If both copies are corrupt, the API returns `500 DATA_CORRUPTED` instead of bad bytes |

To run any step by hand:

```bash
docker stop worker2 ; ./bin/dosctl cluster ; ./bin/dosctl replication ; ./bin/dosctl download <id>
docker start worker2 ; ./bin/dosctl replication
scripts/corrupt_chunk.sh worker1 <chunk_id> ; ./bin/dosctl download <id> ; docker compose logs controller | grep REPLICA_CORRUPT
```

Every controller decision is logged as a structured key=value event (`msg=WORKER_DEAD`, `msg=REPAIR_DONE`, …): `make logs`.

## Configuration

All settings are environment variables in `docker-compose.yml`. Durations are in seconds (`2`) or Go format (`500ms`).

| Variable | Default | Meaning |
|---|---|---|
| `CHUNK_SIZE` | 1048576 | Bytes per chunk |
| `REPLICATION_FACTOR` | 2 | Copies per chunk |
| `HEARTBEAT_INTERVAL` | 2 | Seconds between worker heartbeats |
| `SUSPECT_AFTER` / `DEAD_AFTER` | 5 / 10 | Seconds of heartbeat silence before a worker is marked SUSPECT / DEAD |
| `REPAIR_GRACE` | 20 | How long a worker may stay DEAD before its chunks are re-replicated |
| `REPAIR_INTERVAL` | 5 | Seconds between repair passes (also triggered by events) |
| `SCRUB_INTERVAL` | 60 | Seconds between background checksum scrubs on each worker |
| `ORPHAN_MIN_AGE` | 60 | Minimum age before an unreferenced chunk file is garbage-collected |

## Tests

```bash
make test               # unit + in-process cluster tests (~1 s)
make race               # same, with the race detector
make integration        # against the running compose stack
make integration-slow   # + real docker stop/start of worker2 (~1 min)
```

The in-process tests start a real controller and 4 real workers on `httptest` servers. Any worker can be made to drop its connections. They cover:

- chunk boundary sizes;
- placement spread;
- upload failover;
- dead-worker re-replication and its return with trimming;
- read-repair, the scrubber, missing chunk files, and orphans;
- a controller restart.

## Repository layout

```
cmd/controller, cmd/worker, cmd/dosctl   entrypoints (signal.NotifyContext, graceful shutdown; cobra CLI)
internal/api          shared DTOs
internal/config       env parsing with defaults
internal/httpx        JSON helpers, error envelope, APIError -> status mapping
internal/checksum     sha256 helpers
internal/controller   routes, object handlers, placement, membership, repair, GC, worker client
internal/controller/store   embedded schema.sql, SQLite setup, all metadata queries
internal/worker       chunk store, internal API, heartbeat agent, scrubber
internal/client       Go SDK for the public API (used by dosctl and the integration tests)
scripts/              demo.sh, corrupt_chunk.sh, gen_samples.sh
tests/integration/    //go:build integration -- drives the compose stack
```
