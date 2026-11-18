#!/usr/bin/env bash
# Guided walkthrough of the six required demonstrations.
#
#   scripts/demo.sh            # interactive: pauses between steps
#   AUTO=1 scripts/demo.sh     # run straight through
#   scripts/demo.sh 5          # run only demo #5 (worker failure)
#
# Uses ./bin/dosctl (make build) or dosctl on PATH, otherwise the client container.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
[[ -f samples/random-5.5MB.bin ]] || scripts/gen_samples.sh >/dev/null
export DOS_URL="${DOS_URL:-http://localhost:8000}"
ONLY="${1:-}"

if [[ -x "$ROOT/bin/dosctl" ]]; then
  dos() { (cd "$ROOT/samples" && "$ROOT/bin/dosctl" "$@"); }
elif command -v dosctl >/dev/null 2>&1; then
  dos() { (cd "$ROOT/samples" && dosctl "$@"); }
else
  dos() { docker compose -f "$ROOT/docker-compose.yml" run --rm -T client "$@"; }
fi
logs() { docker compose logs controller --since "$1" | grep -E "$2" | sed -E 's/^controller +\| //; s/ component=controller//' || true; }
json() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }

B=$'\e[1m'; G=$'\e[32m'; Y=$'\e[33m'; C=$'\e[36m'; R=$'\e[0m'
title() { echo; echo "${B}${C}══════════ $* ══════════${R}"; }
say()   { echo "${Y}▶ $*${R}"; }
run()   { echo "${B}\$ $*${R}"; "$@"; }
pause() { [[ -n "${AUTO:-}" ]] || read -rp "${G}[enter to continue]${R} " _; }
want()  { [[ -z "$ONLY" || "$ONLY" == "$1" ]]; }
wait_for() {  # wait_for "description" timeout_s command...
  local desc="$1" t="$2"; shift 2
  printf "%s" "${Y}… waiting: $desc${R}"
  for ((i = 0; i < t; i++)); do
    if "$@" >/dev/null 2>&1; then echo " ${G}(after ${i}s)${R}"; return 0; fi
    sleep 1; printf "."
  done
  echo " timed out"; return 1
}
worker_status() { curl -fsS "$DOS_URL/api/v1/cluster/workers" | json "[w['status'] for w in d['workers'] if w['worker_id']=='$1'][0]"; }
is_status() { [[ "$(worker_status "$1")" == "$2" ]]; }
healthy_all() { curl -fsS "$DOS_URL/api/v1/cluster/replication?details=false" | json "d['healthy']==d['total_chunks'] and d['over_replicated']==0" | grep -q True; }
under_rep() { curl -fsS "$DOS_URL/api/v1/cluster/replication?details=false" | json "d['under_replicated']" | grep -qv '^0$'; }

title "Cluster"
run dos cluster
pause

# ---------------------------------------------------------------- 1
if want 1; then
title "1. Object storage: upload, download, verify"
say "upload a text file and an image"
TXT=$(dos upload hello.txt --json | json "d['object_id']")
PNG=$(dos upload gradient.png --json | json "d['object_id']")
echo "hello.txt    -> $TXT"; echo "gradient.png -> $PNG"
say "download both and compare byte-for-byte with the originals"
run dos download "$TXT" -o hello.out.txt
run dos download "$PNG" -o gradient.out.png
run cmp samples/hello.txt samples/hello.out.txt && echo "${G}hello.txt identical${R}"
run cmp samples/gradient.png samples/gradient.out.png && echo "${G}gradient.png identical${R}"
run dos ls
pause
fi

# ---------------------------------------------------------------- 2 + 3
if want 2 || want 3; then
title "2. Chunking & distribution (5.5 MiB -> 6 x 1 MiB chunks)"
OBJ=$(dos upload random-5.5MB.bin --json | json "d['object_id']")
echo "object_id: $OBJ"
run dos stat "$OBJ"
say "chunk files actually on each worker's persistent volume:"
for w in worker1 worker2 worker3 worker4; do
  n=$(docker exec "$w" sh -c "find /data/chunks -name '$OBJ-*.bin' | wc -l")
  echo "  $w: $n chunk file(s) of this object"
done
pause

title "3. Replication: every chunk has 2 copies on different workers"
curl -fsS "$DOS_URL/api/v1/objects/$OBJ/metadata" | python3 -c '
import sys, json
d = json.load(sys.stdin)
for c in d["chunks"]:
    ws = [r["worker_id"] for r in c["replicas"] if r["state"] == "OK"]
    ok = "OK " if len(set(ws)) == 2 else "BAD"
    idx, cid, where = c["index"], c["chunk_id"], ", ".join(ws)
    print(f"  [{ok}] chunk {idx}  {cid}  ->  {where}")'
run dos replication
pause
fi

# ---------------------------------------------------------------- 4
if want 4; then
title "4. Metadata: custom key/values, update without re-upload"
META=$(dos upload random-5.5MB.bin -m owner=alice -m project=cloud-course -m version=1 --json | json "d['object_id']")
run dos meta get "$META"
before=$(curl -fsS "$DOS_URL/api/v1/objects/$META/metadata" | json "d['last_modified']")
files_before=$(for w in worker1 worker2 worker3 worker4; do docker exec $w sh -c "stat -c '%n %s %Y' \$(find /data/chunks -name '$META-*.bin') 2>/dev/null"; done | md5sum)
sleep 1
say "modify: set version=2, add reviewed=true, remove owner"
run dos meta set "$META" version=2 reviewed=true
run dos meta rm "$META" owner
say "raw PATCH via curl (JSON merge-patch)"
run curl -fsS -X PATCH "$DOS_URL/api/v1/objects/$META/metadata" -H 'Content-Type: application/json' -d '{"status":"approved"}' -o /dev/null -w "HTTP %{http_code}\n"
after=$(curl -fsS "$DOS_URL/api/v1/objects/$META/metadata" | json "d['last_modified']")
files_after=$(for w in worker1 worker2 worker3 worker4; do docker exec $w sh -c "stat -c '%n %s %Y' \$(find /data/chunks -name '$META-*.bin') 2>/dev/null"; done | md5sum)
echo "last_modified: $before -> $after"
[[ "$files_before" == "$files_after" ]] && echo "${G}chunk files untouched: metadata update did not re-upload any data${R}"
run dos stat "$META"
pause
fi

# ---------------------------------------------------------------- 5
if want 5; then
title "5. Worker failure: docker stop worker2 / docker start worker2"
VID=$(dos upload video-20MB.mp4 --json | json "d['object_id']")
echo "uploaded video-20MB.mp4 (20 chunks): $VID"
say "BEFORE"
run dos cluster
run dos replication
pause

run docker stop worker2
wait_for "controller marks worker2 DEAD (heartbeat timeout 10s)" 30 is_status worker2 DEAD
say "DURING: worker2 is down; chunks that had a copy there are under-replicated"
run dos cluster
run dos replication
say "the object is still fully retrievable from the surviving copies"
run dos download "$VID" -o video.during.mp4
pause

wait_for "re-replication after the 20s grace period" 60 healthy_all
logs 60s "WORKER_(SUSPECT|DEAD)|REPAIR_PASS"
say "RF=2 restored on the 3 remaining workers while worker2 is still down"
run dos replication
pause

run docker start worker2
wait_for "worker2 re-registers" 30 is_status worker2 ALIVE
sleep 3  # let reconciliation verify worker2's old copies
wait_for "reconciliation + trimming of extra copies" 30 healthy_all
logs 30s "WORKER_REGISTERED|RECONCILED|REPAIR_PASS"
say "AFTER: worker2 back, every chunk has exactly 2 copies again"
run dos cluster
run dos replication
run dos download "$VID" -o video.after.mp4
pause
fi

# ---------------------------------------------------------------- 6
if want 6; then
title "6. Data integrity: intentional corruption"
OBJ=$(dos upload random-5.5MB.bin --json | json "d['object_id']")
read -r CID W < <(curl -fsS "$DOS_URL/api/v1/objects/$OBJ/metadata" | json "d['chunks'][2]['chunk_id'] + ' ' + sorted(r['worker_id'] for r in d['chunks'][2]['replicas'])[0]")
say "(a) corrupt chunk 2 on $W (the replica the controller reads first), then download"
run scripts/corrupt_chunk.sh "$W" "$CID"
run dos download "$OBJ" -o integrity.out.bin
sleep 2
logs 5s "REPLICA_CORRUPT|REPAIR_DONE"
echo "${G}corruption detected by checksum, a valid copy was served, and the bad copy was rewritten${R}"
pause

read -r CID W < <(curl -fsS "$DOS_URL/api/v1/objects/$OBJ/metadata" | json "d['chunks'][4]['chunk_id'] + ' ' + d['chunks'][4]['replicas'][1]['worker_id']")
say "(b) corrupt chunk 4 on $W and find it WITHOUT reading the object: background scrubber (forced now)"
run scripts/corrupt_chunk.sh "$W" "$CID"
run dos scrub
run dos replication
pause

say "(c) corrupt BOTH copies of a chunk: the system refuses to return bad data"
BAD=$(dos upload random-5.5MB.bin --json | json "d['object_id']")
for w in $(curl -fsS "$DOS_URL/api/v1/objects/$BAD/metadata" | json "' '.join(r['worker_id'] for r in d['chunks'][0]['replicas'])"); do
  scripts/corrupt_chunk.sh "$w" "$BAD-c00000" | head -1
done
run curl -sS "$DOS_URL/api/v1/objects/$BAD" -o /dev/null -w "HTTP %{http_code}\n"
run curl -sS "$DOS_URL/api/v1/objects/$BAD"; echo
run dos rm "$BAD"
fi

title "Done"
