#!/usr/bin/env bash
# End-to-end demo: record -> export -> montage-by-reference -> live follow.
# Prereqs: docker compose up -d   (store on :8000), ffmpeg/ffprobe on PATH,
#          pip install -e .       (or use the .venv entry points)
set -euo pipefail

TAMS=${TAMS:-http://localhost:8000}
FLOW=$(uuidgen | tr 'A-Z' 'a-z')
SRC=$(uuidgen | tr 'A-Z' 'a-z')
OUT=${OUT:-out}
mkdir -p "$OUT"

echo "== 1. RECORD: 16s of testsrc into TAMS (2s chunks) =="
tams-record --tams "$TAMS" --source testsrc --duration 16 \
  --flow-id "$FLOW" --source-id "$SRC" --label "demo recording"

TR=$(curl -s "$TAMS/flows/$FLOW?include_timerange=true" | python3 -c "import json,sys; print(json.load(sys.stdin)['timerange'])")
echo "recorded timerange: $TR"

echo
echo "== 2. EXPORT: materialise the whole flow to a .ts file =="
time tams-consume --tams "$TAMS" --flow-id "$FLOW" --timerange "$TR" --out "$OUT/export.ts"
ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1 "$OUT/export.ts"

echo
echo "== 3. MONTAGE: 3 clips by reference (zero copy), then materialise =="
START_NS=$(python3 -c "
from tamslite import timeline as tl
r = tl.timerange_to_ns('$TR')
print(r.start_ns)")
CLIP() { python3 -c "
from tamslite import timeline as tl
print(tl.ns_to_timerange($START_NS + $1*tl.NS, $START_NS + $2*tl.NS))"; }
MONTAGE=$(tams-montage --tams "$TAMS" --src-flow-id "$FLOW" \
  --clip "$(CLIP 0 2)" --clip "$(CLIP 6 8)" --clip "$(CLIP 12 14)")
tams-consume --tams "$TAMS" --flow-id "$MONTAGE" --timerange "[0:0_6:0)" --remux --out "$OUT/montage.ts"
ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1 "$OUT/montage.ts"

echo
echo "== 4. LIVE: record 20s in background, consume open-ended, paced =="
LFLOW=$(uuidgen | tr 'A-Z' 'a-z')
tams-record --tams "$TAMS" --source testsrc --duration 20 \
  --flow-id "$LFLOW" --source-id "$(uuidgen | tr 'A-Z' 'a-z')" --label "demo live" &
REC_PID=$!
sleep 5
LSTART=$(curl -s "$TAMS/flows/$LFLOW?include_timerange=true" | python3 -c "import json,sys; print(json.load(sys.stdin)['timerange'].split('_')[0].lstrip('['))")
tams-consume --tams "$TAMS" --flow-id "$LFLOW" --timerange "[${LSTART}_" \
  --until-idle 6 --out "$OUT/live.ts"
wait $REC_PID
ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1 "$OUT/live.ts"

echo
echo "demo complete: $OUT/export.ts $OUT/montage.ts $OUT/live.ts"
