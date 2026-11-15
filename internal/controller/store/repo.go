package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gathik-jindal/mini-distributed-object-store/internal/api"
)

// Replica states and object statuses.
const (
	OK      = "OK"
	Corrupt = "CORRUPT"
	Missing = "MISSING"
	Stale   = "STALE"

	Uploading = "UPLOADING"
	Committed = "COMMITTED"
	Deleting  = "DELETING"
)

var (
	ErrNotFound = errors.New("not found")
	ErrBusy     = errors.New("object busy")
)

// NowISO is the timestamp format stored in the DB (UTC, millisecond precision).
func NowISO() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }

func nowUnix() float64 { return float64(time.Now().UnixNano()) / 1e9 }

type Object struct {
	ObjectID       string
	Name           string
	Size           int64
	ContentType    string
	Checksum       string
	ChunkSize      int
	ChunkCount     int
	CustomMetadata map[string]any
	Status         string
	CreatedAt      string
	UpdatedAt      string
}

type ChunkRow struct {
	ChunkID  string
	ObjectID string
	Index    int
	Size     int64
	Checksum string
}

type Replica struct {
	WorkerID string
	State    string
}

// Chunk is a chunk together with all its replica rows.
type Chunk struct {
	ChunkRow
	Replicas []Replica
}

type WorkerRow struct {
	WorkerID      string
	URL           string
	Status        string
	LastHeartbeat float64
	RegisteredAt  float64
	UsedBytes     int64
	FreeBytes     int64
	ChunkCount    int
}

type PendingDelete struct {
	ChunkID  string
	WorkerID string
	Attempts int
}

// Repo holds every metadata query.
type Repo struct{ db *sql.DB }

func NewRepo(db *sql.DB) *Repo { return &Repo{db: db} }

func (r *Repo) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

// ------------------------------------------------------------------ objects

func (r *Repo) CreateObject(ctx context.Context, id, name, contentType string, chunkSize int, meta map[string]any) error {
	mj, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	now := NowISO()
	_, err = r.db.ExecContext(ctx, `INSERT INTO objects(object_id,name,size,content_type,checksum,chunk_size,
		chunk_count,custom_metadata,status,created_at,updated_at) VALUES(?,?,0,?,NULL,?,0,?,?,?,?)`,
		id, name, contentType, chunkSize, string(mj), Uploading, now, now)
	return err
}

// CommitObject inserts chunks + replica rows and marks the object COMMITTED, atomically.
func (r *Repo) CommitObject(ctx context.Context, id string, size int64, sum string, chunks []ChunkRow, placements map[string][]string) error {
	now := NowISO()
	return r.tx(ctx, func(tx *sql.Tx) error {
		for _, c := range chunks {
			if _, err := tx.ExecContext(ctx, `INSERT INTO chunks(chunk_id,object_id,idx,size,checksum) VALUES(?,?,?,?,?)`,
				c.ChunkID, id, c.Index, c.Size, c.Checksum); err != nil {
				return err
			}
			for _, w := range placements[c.ChunkID] {
				if _, err := tx.ExecContext(ctx, `INSERT INTO replicas(chunk_id,worker_id,state,updated_at) VALUES(?,?,?,?)`,
					c.ChunkID, w, OK, now); err != nil {
					return err
				}
			}
		}
		_, err := tx.ExecContext(ctx, `UPDATE objects SET size=?, checksum=?, chunk_count=?, status=?, updated_at=? WHERE object_id=?`,
			size, sum, len(chunks), Committed, now, id)
		return err
	})
}

func (r *Repo) AbortObject(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM objects WHERE object_id=?`, id)
	return err
}

func scanObject(row interface{ Scan(...any) error }) (*Object, error) {
	var o Object
	var sum sql.NullString
	var meta string
	if err := row.Scan(&o.ObjectID, &o.Name, &o.Size, &o.ContentType, &sum, &o.ChunkSize, &o.ChunkCount,
		&meta, &o.Status, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return nil, err
	}
	o.Checksum = sum.String
	o.CustomMetadata = map[string]any{}
	dec := json.NewDecoder(strings.NewReader(meta))
	dec.UseNumber()
	_ = dec.Decode(&o.CustomMetadata)
	return &o, nil
}

const objectCols = `object_id,name,size,content_type,checksum,chunk_size,chunk_count,custom_metadata,status,created_at,updated_at`

// GetObject returns nil, nil when the object doesn't exist.
func (r *Repo) GetObject(ctx context.Context, id string) (*Object, error) {
	o, err := scanObject(r.db.QueryRowContext(ctx, `SELECT `+objectCols+` FROM objects WHERE object_id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return o, err
}

func (r *Repo) ListObjects(ctx context.Context, limit, offset int) ([]api.ObjectSummary, int, error) {
	var total int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM objects WHERE status=?`, Committed).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT object_id,name,size,content_type,COALESCE(checksum,''),chunk_count,
		created_at,updated_at FROM objects WHERE status=? ORDER BY created_at, object_id LIMIT ? OFFSET ?`,
		Committed, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := []api.ObjectSummary{}
	for rows.Next() {
		var s api.ObjectSummary
		if err := rows.Scan(&s.ObjectID, &s.Name, &s.Size, &s.ContentType, &s.Checksum, &s.ChunkCount,
			&s.CreatedAt, &s.LastModified); err != nil {
			return nil, 0, err
		}
		items = append(items, s)
	}
	return items, total, rows.Err()
}

// PatchCustomMetadata applies a JSON merge-patch (nil value removes the key).
func (r *Repo) PatchCustomMetadata(ctx context.Context, id string, patch map[string]any) (map[string]any, error) {
	var out map[string]any
	err := r.tx(ctx, func(tx *sql.Tx) error {
		o, err := scanObject(tx.QueryRowContext(ctx, `SELECT `+objectCols+` FROM objects WHERE object_id=?`, id))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		if o.Status != Committed {
			return fmt.Errorf("%w: object %s is %s", ErrBusy, id, o.Status)
		}
		for k, v := range patch {
			if v == nil {
				delete(o.CustomMetadata, k)
			} else {
				o.CustomMetadata[k] = v
			}
		}
		mj, err := json.Marshal(o.CustomMetadata)
		if err != nil {
			return err
		}
		out = o.CustomMetadata
		_, err = tx.ExecContext(ctx, `UPDATE objects SET custom_metadata=?, updated_at=? WHERE object_id=?`, string(mj), NowISO(), id)
		return err
	})
	return out, err
}

// DeleteObject tombstones an object: every replica is queued for deletion on
// its worker and all metadata is dropped in the same transaction.
func (r *Repo) DeleteObject(ctx context.Context, id string) (int, error) {
	n := 0
	err := r.tx(ctx, func(tx *sql.Tx) error {
		var status string
		err := tx.QueryRowContext(ctx, `SELECT status FROM objects WHERE object_id=?`, id).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		if status == Uploading {
			return fmt.Errorf("%w: object %s is still uploading", ErrBusy, id)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET status=? WHERE object_id=?`, Deleting, id); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes(chunk_id,worker_id,attempts,created_at)
			SELECT r.chunk_id, r.worker_id, 0, ? FROM replicas r JOIN chunks c ON c.chunk_id=r.chunk_id WHERE c.object_id=?`,
			nowUnix(), id)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		n = int(affected)
		for _, q := range []string{
			`DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=?)`,
			`DELETE FROM chunks WHERE object_id=?`,
			`DELETE FROM objects WHERE object_id=?`,
		} {
			if _, err := tx.ExecContext(ctx, q, id); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

func (r *Repo) UploadingObjectIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT object_id FROM objects WHERE status=?`, Uploading)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// DropStaleUploads removes UPLOADING/DELETING rows left behind by a crash
// (older than olderThan, or all of them when olderThan is ""). Their partially
// written chunks have no replica rows and are later removed as orphans.
func (r *Repo) DropStaleUploads(ctx context.Context, olderThan string) ([]string, error) {
	var ids []string
	err := r.tx(ctx, func(tx *sql.Tx) error {
		q := `SELECT object_id FROM objects WHERE status IN (?,?)`
		args := []any{Uploading, Deleting}
		if olderThan != "" {
			q += ` AND updated_at < ?`
			args = append(args, olderThan)
		}
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		for _, id := range ids {
			for _, q := range []string{
				`DELETE FROM replicas WHERE chunk_id IN (SELECT chunk_id FROM chunks WHERE object_id=?)`,
				`DELETE FROM chunks WHERE object_id=?`,
				`DELETE FROM objects WHERE object_id=?`,
			} {
				if _, err := tx.ExecContext(ctx, q, id); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return ids, err
}

// ------------------------------------------------------------------ chunks & replicas

func (r *Repo) queryChunks(ctx context.Context, where string, args ...any) ([]Chunk, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT c.chunk_id, c.object_id, c.idx, c.size, c.checksum, r.worker_id, r.state
		FROM chunks c JOIN objects o ON o.object_id=c.object_id
		LEFT JOIN replicas r ON r.chunk_id=c.chunk_id WHERE `+where+` ORDER BY c.object_id, c.idx, r.worker_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Chunk
	pos := map[string]int{}
	for rows.Next() {
		var c ChunkRow
		var wid, state sql.NullString
		if err := rows.Scan(&c.ChunkID, &c.ObjectID, &c.Index, &c.Size, &c.Checksum, &wid, &state); err != nil {
			return nil, err
		}
		i, ok := pos[c.ChunkID]
		if !ok {
			i = len(out)
			pos[c.ChunkID] = i
			out = append(out, Chunk{ChunkRow: c, Replicas: []Replica{}})
		}
		if wid.Valid {
			out[i].Replicas = append(out[i].Replicas, Replica{WorkerID: wid.String, State: state.String})
		}
	}
	return out, rows.Err()
}

// ObjectChunks returns an object's chunks in index order with their replicas.
func (r *Repo) ObjectChunks(ctx context.Context, objectID string) ([]Chunk, error) {
	cs, err := r.queryChunks(ctx, `c.object_id=?`, objectID)
	sort.Slice(cs, func(i, j int) bool { return cs[i].Index < cs[j].Index })
	return cs, err
}

// AllChunks returns every chunk of every committed object.
func (r *Repo) AllChunks(ctx context.Context) ([]Chunk, error) {
	return r.queryChunks(ctx, `o.status=?`, Committed)
}

// SetReplicaState changes one replica's state, optionally only from given states.
func (r *Repo) SetReplicaState(ctx context.Context, chunkID, workerID, state string, onlyFrom ...string) (bool, error) {
	q := `UPDATE replicas SET state=?, updated_at=? WHERE chunk_id=? AND worker_id=?`
	args := []any{state, NowISO(), chunkID, workerID}
	if len(onlyFrom) > 0 {
		q += ` AND state IN (` + placeholders(len(onlyFrom)) + `)`
		for _, s := range onlyFrom {
			args = append(args, s)
		}
	}
	res, err := r.db.ExecContext(ctx, q, args...)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (r *Repo) SetReplicaStates(ctx context.Context, workerID string, states map[string]string) error {
	if len(states) == 0 {
		return nil
	}
	now := NowISO()
	return r.tx(ctx, func(tx *sql.Tx) error {
		for cid, st := range states {
			if _, err := tx.ExecContext(ctx, `UPDATE replicas SET state=?, updated_at=? WHERE chunk_id=? AND worker_id=?`,
				st, now, cid, workerID); err != nil {
				return err
			}
		}
		return nil
	})
}

// AddReplicaOK records a freshly written replica. It returns false (and queues
// the copy for deletion) if the chunk was deleted in the meantime.
func (r *Repo) AddReplicaOK(ctx context.Context, chunkID, workerID string) (bool, error) {
	ok := false
	err := r.tx(ctx, func(tx *sql.Tx) error {
		var x string
		err := tx.QueryRowContext(ctx, `SELECT chunk_id FROM chunks WHERE chunk_id=?`, chunkID).Scan(&x)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes(chunk_id,worker_id,attempts,created_at) VALUES(?,?,0,?)`,
				chunkID, workerID, nowUnix())
			return err
		} else if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO replicas(chunk_id,worker_id,state,updated_at) VALUES(?,?,?,?)
			ON CONFLICT(chunk_id,worker_id) DO UPDATE SET state=excluded.state, updated_at=excluded.updated_at`,
			chunkID, workerID, OK, NowISO()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?`, chunkID, workerID)
		ok = err == nil
		return err
	})
	return ok, err
}

// DropReplica removes a replica row and optionally queues the copy for deletion.
func (r *Repo) DropReplica(ctx context.Context, chunkID, workerID string, queueDelete bool) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM replicas WHERE chunk_id=? AND worker_id=?`, chunkID, workerID); err != nil {
			return err
		}
		if !queueDelete {
			return nil
		}
		_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes(chunk_id,worker_id,attempts,created_at) VALUES(?,?,0,?)`,
			chunkID, workerID, nowUnix())
		return err
	})
}

// WorkerReplicas lists every replica row the metadata expects on a worker.
func (r *Repo) WorkerReplicas(ctx context.Context, workerID string) ([]api.ExpectedChunk, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT r.chunk_id, r.state, c.object_id, c.idx, c.size, c.checksum
		FROM replicas r JOIN chunks c ON c.chunk_id=r.chunk_id WHERE r.worker_id=? ORDER BY c.object_id, c.idx`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []api.ExpectedChunk{}
	for rows.Next() {
		var e api.ExpectedChunk
		if err := rows.Scan(&e.ChunkID, &e.State, &e.ObjectID, &e.Index, &e.Size, &e.Checksum); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *Repo) ReplicaCountsByWorker(ctx context.Context) (map[string]int, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT worker_id, COUNT(*) FROM replicas WHERE state=? GROUP BY worker_id`, OK)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var w string
		var n int
		if err := rows.Scan(&w, &n); err != nil {
			return nil, err
		}
		out[w] = n
	}
	return out, rows.Err()
}

// ------------------------------------------------------------------ workers

func (r *Repo) UpsertWorker(ctx context.Context, w WorkerRow) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO workers(worker_id,url,status,last_heartbeat,registered_at,used_bytes,free_bytes,chunk_count)
		VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET url=excluded.url, status=excluded.status,
		last_heartbeat=excluded.last_heartbeat, registered_at=excluded.registered_at, used_bytes=excluded.used_bytes,
		free_bytes=excluded.free_bytes, chunk_count=excluded.chunk_count`,
		w.WorkerID, w.URL, w.Status, w.LastHeartbeat, w.RegisteredAt, w.UsedBytes, w.FreeBytes, w.ChunkCount)
	return err
}

func (r *Repo) UpdateWorkerStatus(ctx context.Context, workerID, status string, lastHeartbeat float64) error {
	_, err := r.db.ExecContext(ctx, `UPDATE workers SET status=?, last_heartbeat=COALESCE(NULLIF(?,0),last_heartbeat) WHERE worker_id=?`,
		status, lastHeartbeat, workerID)
	return err
}

func (r *Repo) LoadWorkers(ctx context.Context) ([]WorkerRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT worker_id,url,status,COALESCE(last_heartbeat,0),COALESCE(registered_at,0),
		used_bytes,free_bytes,chunk_count FROM workers ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerRow
	for rows.Next() {
		var w WorkerRow
		if err := rows.Scan(&w.WorkerID, &w.URL, &w.Status, &w.LastHeartbeat, &w.RegisteredAt, &w.UsedBytes,
			&w.FreeBytes, &w.ChunkCount); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ------------------------------------------------------------------ pending deletes

func (r *Repo) QueueDelete(ctx context.Context, chunkID, workerID string) error {
	_, err := r.db.ExecContext(ctx, `INSERT OR IGNORE INTO pending_deletes(chunk_id,worker_id,attempts,created_at) VALUES(?,?,0,?)`,
		chunkID, workerID, nowUnix())
	return err
}

// PendingDeletes lists queued deletes, optionally only for the given workers.
func (r *Repo) PendingDeletes(ctx context.Context, workerIDs []string, limit int) ([]PendingDelete, error) {
	q := `SELECT chunk_id, worker_id, attempts FROM pending_deletes`
	var args []any
	if workerIDs != nil {
		if len(workerIDs) == 0 {
			return nil, nil
		}
		q += ` WHERE worker_id IN (` + placeholders(len(workerIDs)) + `)`
		for _, w := range workerIDs {
			args = append(args, w)
		}
	}
	q += ` ORDER BY created_at LIMIT ?`
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingDelete
	for rows.Next() {
		var p PendingDelete
		if err := rows.Scan(&p.ChunkID, &p.WorkerID, &p.Attempts); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *Repo) PendingDeleteCount(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_deletes`).Scan(&n)
	return n, err
}

func (r *Repo) FinishDelete(ctx context.Context, chunkID, workerID string, ok bool) error {
	q := `UPDATE pending_deletes SET attempts=attempts+1 WHERE chunk_id=? AND worker_id=?`
	if ok {
		q = `DELETE FROM pending_deletes WHERE chunk_id=? AND worker_id=?`
	}
	_, err := r.db.ExecContext(ctx, q, chunkID, workerID)
	return err
}
