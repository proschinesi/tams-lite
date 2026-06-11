"""tams-record: ingest a source, segment to immutable mpegts chunks, register in TAMS.

The feed is recorded AS-IS: by default ffmpeg stream-copies (`-c copy`, no
re-encode), so the bytes stored are exactly the source's coded essence, just
re-wrapped into mpegts segments cut at the source's existing GOP boundaries.
The Flow metadata (codec, resolution, frame rate) is *probed* from the source
rather than assumed, so TAMS describes what was actually recorded. Re-encoding
only happens for synthetic raw sources (`testsrc`, which has no coded essence)
or when the user explicitly asks for it with `--encode`.

ffmpeg's segment muxer writes ~N second .ts chunks; in copy mode each cut lands
on an existing keyframe, so chunk durations follow the source GOP and are read
back exactly per chunk via ffprobe. For each completed chunk we:
  1. take a presigned PUT url from a pre-allocated pool (POST /flows/{id}/storage)
  2. upload the bytes to the object store (TAMS never sees the media)
  3. POST the Flow Segment: timerange on the TAI wall-clock timeline + ts_offset
     mapping the chunk's internal PTS to that timeline (segment_ts = media_ts + ts_offset)

Timeline model: the wall-clock TAI instant when ffmpeg starts is the anchor for
media time 0 of the recording; chunk k covers [anchor + pts_start_k, anchor +
pts_start_{k+1}), which keeps the flow timeline gap-free by construction.
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


# ffmpeg codec_name -> codec MIME type for the Flow `codec` field (IANA-style).
_CODEC_MIME = {
    "h264": "video/h264",
    "hevc": "video/h265",
    "h265": "video/h265",
    "mpeg2video": "video/mpeg2",
    "vp8": "video/vp8",
    "vp9": "video/vp9",
    "av1": "video/av01",
}


def probe_source(source: str) -> dict:
    """Probe the source video stream so the Flow metadata describes what is
    actually recorded (codec, resolution, frame rate, interlacing)."""
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=codec_name,width,height,r_frame_rate,field_order",
         "-of", "json", source],
        capture_output=True, text=True, check=True,
    ).stdout
    streams = json.loads(out).get("streams", [])
    if not streams:
        raise SystemExit(f"no video stream found in source: {source}")
    s = streams[0]
    num, _, den = s.get("r_frame_rate", "25/1").partition("/")
    field = s.get("field_order", "progressive")
    interlace = "progressive" if field in ("progressive", "", "unknown") else (
        "interlaced_tff" if field in ("tt", "tb") else "interlaced_bff"
    )
    return {
        "codec_name": s["codec_name"],
        "width": int(s["width"]),
        "height": int(s["height"]),
        "rate_num": int(num),
        "rate_den": int(den) if den else 1,
        "interlace": interlace,
    }


def build_flow_doc(flow_id: str, source_id: str, label: str, params: dict, chunk: int) -> dict:
    codec = _CODEC_MIME.get(params["codec_name"], f"video/{params['codec_name']}")
    return {
        "id": flow_id,
        "source_id": source_id,
        "label": label,
        "format": "urn:x-nmos:format:video",
        "codec": codec,
        "container": "video/mp2t",
        "generation": 0,
        "segment_duration": {"numerator": chunk, "denominator": 1},
        "essence_parameters": {
            "frame_width": params["width"],
            "frame_height": params["height"],
            "frame_rate": {"numerator": params["rate_num"], "denominator": params["rate_den"]},
            "interlace_mode": params["interlace"],
        },
        "tags": {"recorded_by": "tams-lite-recorder"},
    }


def ffmpeg_cmd(args, outdir: Path) -> list[str]:
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if args.source == "testsrc":
        # synthetic raw source: it has no coded essence, so it must be encoded
        if args.duration:
            cmd += ["-t", str(args.duration)]
        cmd += ["-re", "-f", "lavfi",
                "-i", f"testsrc2=size={args.width}x{args.height}:rate={args.rate}"]
        gop = args.rate * args.chunk
        codec = ["-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
                 "-b:v", args.bitrate, "-g", str(gop), "-keyint_min", str(gop),
                 "-sc_threshold", "0"]
    else:
        cmd += ["-re", "-i", args.source]
        if args.duration:
            cmd += ["-t", str(args.duration)]
        if args.encode:
            # opt-in transcode (e.g. to normalise a feed); changes the bytes
            gop = args.rate * args.chunk
            codec = ["-c:v", "libx264", "-preset", "veryfast",
                     "-b:v", args.bitrate, "-g", str(gop), "-keyint_min", str(gop),
                     "-sc_threshold", "0"]
        else:
            # DEFAULT: record the feed as-is — no re-encode, just re-wrap to mpegts
            codec = ["-c", "copy"]
    return [
        *cmd, "-an", *codec,
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
    ap.add_argument("--encode", action="store_true",
                    help="re-encode the feed instead of recording it as-is (default: stream-copy, no re-encode)")
    ap.add_argument("--width", type=int, default=1280, help="testsrc / --encode only")
    ap.add_argument("--height", type=int, default=720, help="testsrc / --encode only")
    ap.add_argument("--rate", type=int, default=25, help="testsrc / --encode only")
    ap.add_argument("--bitrate", default="2000k", help="testsrc / --encode only")
    args = ap.parse_args()

    flow_id = args.flow_id or str(uuid.uuid4())
    source_id = args.source_id or str(uuid.uuid4())
    client = TamsClient(args.tams)

    if args.source == "testsrc":
        # synthetic source: parameters come from the CLI (we are generating it)
        params = {"codec_name": "h264", "width": args.width, "height": args.height,
                  "rate_num": args.rate, "rate_den": 1, "interlace": "progressive"}
    elif args.encode:
        # transcoding to the requested target: describe the target, not the source
        params = {"codec_name": "h264", "width": args.width, "height": args.height,
                  "rate_num": args.rate, "rate_den": 1, "interlace": "progressive"}
    else:
        # recording as-is: the Flow must describe the source's real essence
        params = probe_source(args.source)
        print(f"source codec: {params['codec_name']} {params['width']}x{params['height']} "
              f"@ {params['rate_num']}/{params['rate_den']} fps (recorded as-is, no re-encode)",
              file=sys.stderr)

    client.put_flow(build_flow_doc(flow_id, source_id, args.label, params, args.chunk))
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
