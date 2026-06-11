"""tams-record: ingest a source, segment to immutable mpegts chunks, register in TAMS.

ffmpeg's segment muxer writes ~N second .ts chunks (GOP aligned, continuity
counters preserved across chunks). For each completed chunk we:
  1. take a presigned PUT url from a pre-allocated pool (POST /flows/{id}/storage)
  2. upload the bytes to the object store (TAMS never sees the media)
  3. POST the Flow Segment: timerange on the TAI wall-clock timeline + ts_offset
     mapping the chunk's internal PTS to that timeline (segment_ts = media_ts + ts_offset)

Timeline model: the wall-clock TAI instant when ffmpeg starts is the anchor for
media time 0 of the encode; chunk k covers [anchor + pts_start_k, anchor + pts_start_{k+1}),
which keeps the flow timeline gap-free by construction.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from .. import timeline as tl
from ..client import TamsClient


def probe_chunk(path: Path) -> tuple[int, int]:
    """Return (first_pts_ns, duration_ns) of an mpegts chunk, exactly."""
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=start_time,duration",
         "-of", "json", str(path)],
        capture_output=True, text=True, check=True,
    ).stdout
    fmt = json.loads(out)["format"]
    return tl.seconds_str_to_ns(fmt["start_time"]), tl.seconds_str_to_ns(fmt["duration"])


def build_flow_doc(flow_id: str, source_id: str, label: str, args) -> dict:
    return {
        "id": flow_id,
        "source_id": source_id,
        "label": label,
        "format": "urn:x-nmos:format:video",
        "codec": "video/h264",
        "container": "video/mp2t",
        "generation": 0,
        "segment_duration": {"numerator": args.chunk, "denominator": 1},
        "essence_parameters": {
            "frame_width": args.width,
            "frame_height": args.height,
            "frame_rate": {"numerator": args.rate, "denominator": 1},
            "interlace_mode": "progressive",
        },
        "tags": {"recorded_by": "tams-lite-recorder"},
    }


def ffmpeg_cmd(args, outdir: Path) -> list[str]:
    if args.source == "testsrc":
        src = ["-re", "-f", "lavfi",
               "-i", f"testsrc2=size={args.width}x{args.height}:rate={args.rate}"]
        if args.duration:
            src = ["-t", str(args.duration), *src]
    else:
        src = ["-re", "-i", args.source]
        if args.duration:
            src += ["-t", str(args.duration)]
    gop = args.rate * args.chunk
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        *src,
        "-an",
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-b:v", args.bitrate, "-g", str(gop), "-keyint_min", str(gop), "-sc_threshold", "0",
        "-f", "segment", "-segment_time", str(args.chunk), "-segment_format", "mpegts",
        str(outdir / "chunk_%05d.ts"),
    ]


class ObjectPool:
    """Pre-allocated {object_id, put_url} entries, topped up in batches."""

    def __init__(self, client: TamsClient, flow_id: str, batch: int = 10) -> None:
        self.client, self.flow_id, self.batch = client, flow_id, batch
        self._pool: list[dict] = []

    def take(self) -> dict:
        if not self._pool:
            self._pool = self.client.allocate_storage(self.flow_id, self.batch)
        return self._pool.pop(0)


def main() -> None:
    ap = argparse.ArgumentParser(description="Record a source into a TAMS store")
    ap.add_argument("--tams", default="http://localhost:8000")
    ap.add_argument("--source", default="testsrc", help="'testsrc', a file path or a URL")
    ap.add_argument("--flow-id", default=None)
    ap.add_argument("--source-id", default=None)
    ap.add_argument("--label", default="tams-lite recording")
    ap.add_argument("--chunk", type=int, default=2, help="target chunk duration, seconds")
    ap.add_argument("--duration", type=float, default=None, help="stop after N seconds")
    ap.add_argument("--width", type=int, default=1280)
    ap.add_argument("--height", type=int, default=720)
    ap.add_argument("--rate", type=int, default=25)
    ap.add_argument("--bitrate", default="2000k")
    args = ap.parse_args()

    flow_id = args.flow_id or str(uuid.uuid4())
    source_id = args.source_id or str(uuid.uuid4())
    client = TamsClient(args.tams)

    client.put_flow(build_flow_doc(flow_id, source_id, args.label, args))
    print(f"flow:   {flow_id}\nsource: {source_id}", file=sys.stderr)

    pool = ObjectPool(client, flow_id)

    with tempfile.TemporaryDirectory(prefix="tams-rec-") as tmp:
        outdir = Path(tmp)
        anchor_ns = tl.now_tai_ns()  # TAI instant of media time ~0
        proc = subprocess.Popen(ffmpeg_cmd(args, outdir))
        print(f"anchor: {tl.ns_to_ts(anchor_ns)} TAI", file=sys.stderr)

        registered = 0
        next_idx = 0
        first_pts_ns: int | None = None
        prev_end_ns: int | None = None

        def chunk_path(i: int) -> Path:
            return outdir / f"chunk_{i:05d}.ts"

        try:
            while True:
                running = proc.poll() is None
                # chunk i is complete once chunk i+1 appears, or ffmpeg has exited
                while chunk_path(next_idx).exists() and (chunk_path(next_idx + 1).exists() or not running):
                    path = chunk_path(next_idx)
                    pts_ns, dur_ns = probe_chunk(path)
                    if first_pts_ns is None:
                        first_pts_ns = pts_ns
                    # segment_ts = media_ts + ts_offset, constant for the whole encode
                    ts_offset_ns = anchor_ns - first_pts_ns
                    start_ns = prev_end_ns if prev_end_ns is not None else pts_ns + ts_offset_ns
                    end_ns = pts_ns + dur_ns + ts_offset_ns
                    entry = pool.take()
                    data = path.read_bytes()
                    client.upload_object(entry["put_url"], data)
                    client.post_segments(flow_id, {
                        "object_id": entry["object_id"],
                        "timerange": tl.ns_to_timerange(start_ns, end_ns),
                        "ts_offset": tl.ns_to_ts(ts_offset_ns),
                        "key_frame_count": 1,
                    })
                    prev_end_ns = end_ns
                    registered += 1
                    print(f"segment {registered}: {tl.ns_to_timerange(start_ns, end_ns)} "
                          f"({len(data)//1024} KiB) -> {entry['object_id']}", file=sys.stderr)
                    path.unlink()
                    next_idx += 1
                    running = proc.poll() is None
                if not running and not chunk_path(next_idx).exists():
                    break
                time.sleep(0.2)
        except KeyboardInterrupt:
            proc.terminate()
            proc.wait()
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait()

    print(f"done: {registered} segments registered", file=sys.stderr)


if __name__ == "__main__":
    main()
