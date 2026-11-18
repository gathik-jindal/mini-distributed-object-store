#!/usr/bin/env bash
# Flip bytes of a stored chunk *in place* inside a worker container, simulating
# silent disk corruption (the sidecar checksum is left untouched).
#
#   scripts/corrupt_chunk.sh <worker> [chunk_id]
#
# Without chunk_id the first chunk found on that worker is corrupted.
set -euo pipefail
W="${1:?usage: $0 <worker> [chunk_id]}"
CID="${2:-}"
if [[ -z "$CID" ]]; then
  F=$(docker exec "$W" sh -c 'find /data/chunks -name "*.bin" | sort | head -n1')
else
  F=$(docker exec "$W" sh -c "find /data/chunks -name '${CID}.bin' | head -n1")
fi
[[ -n "$F" ]] || { echo "no chunk ${CID:-(any)} on $W" >&2; exit 1; }
before=$(docker exec "$W" sha256sum "$F" | cut -c1-16)
docker exec "$W" sh -c "printf 'XXXX-CORRUPTED-BY-DEMO-XXXX' | dd of='$F' bs=1 seek=100 conv=notrunc status=none"
after=$(docker exec "$W" sha256sum "$F" | cut -c1-16)
echo "corrupted $W:$F"
echo "  sha256 before: $before...  after: $after..."
