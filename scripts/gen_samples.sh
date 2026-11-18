#!/usr/bin/env bash
# Create demo files in ./samples (mounted into the client container at /work).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p samples
echo "Hello from the mini distributed object store!" > samples/hello.txt
head -c 5767168 /dev/urandom > samples/random-5.5MB.bin          # 5.5 MiB -> 6 chunks
head -c 20971520 /dev/urandom > samples/video-20MB.mp4           # 20 MiB  -> 20 chunks
python3 - <<'PY'
# tiny valid PNG so content-type detection has something real to chew on
import struct, zlib
def chunk(t, d): return struct.pack(">I", len(d)) + t + d + struct.pack(">I", zlib.crc32(t + d) & 0xffffffff)
w = h = 64
raw = b"".join(b"\x00" + b"".join(bytes((x * 4 % 256, y * 4 % 256, 160)) for x in range(w)) for y in range(h))
png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)) + \
      chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")
open("samples/gradient.png", "wb").write(png)
PY
ls -la samples
