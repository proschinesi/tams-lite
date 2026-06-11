"""tams-consume: resolve a Flow + timerange from TAMS and stream mpegts to a sink.

Modes (decided by the timerange):
  closed  [a_b)  -> materialise as fast as the sink accepts, then EOF (export)
  open    [a_    -> real-time paced live stream; serves what exists, then follows
                    the timeline polling for new segments ("tail -f" of the flow)

One code path for every sink: a sink is just a writable byte stream.
  --out PATH   file (EOF at the end)
  --out -      stdout (pipe to ffplay/vlc/ffmpeg/socat...)
  --listen     minimal single-client HTTP server: GET /stream.ts (chunked)

Edit-by-reference: media bytes flow straight from the object store to the sink;
nothing is ever re-written into new storage.
"""

from __future__ import annotations

import argparse
import sys
import time

from .. import timeline as tl
from ..client import TamsClient


def write_segment(client: TamsClient, seg: dict, sink) -> int:
    n = 0
    for chunk in client.fetch_object(seg):
        sink.write(chunk)
        n += len(chunk)
    return n


class Remuxer:
    """Rewrites mpegts timestamps so edited-by-reference montages play as one
    continuous stream (discontinuity boundaries between re-referenced objects).

    Each segment is remuxed with `ffmpeg -c copy` (no re-encode): the embedded
    PTS (flow time - ts_offset) are shifted onto a continuous output timeline.
    """

    BASE_S = 1.4  # keep first PTS positive, matching ffmpeg's usual ts start

    def __init__(self) -> None:
        self.t0_ns: int | None = None

    def write_segment(self, client: TamsClient, seg: dict, sink) -> int:
        import subprocess

        seg_rng = tl.timerange_to_ns(seg["timerange"])
        if self.t0_ns is None:
            self.t0_ns = seg_rng.start_ns
        media_start_ns = seg_rng.start_ns - tl.ts_to_ns(seg.get("ts_offset", "0:0"))
        desired_s = self.BASE_S + (seg_rng.start_ns - self.t0_ns) / tl.NS
        offset_s = desired_s - media_start_ns / tl.NS
        proc = subprocess.Popen(
            ["ffmpeg", "-hide_banner", "-loglevel", "error",
             "-copyts", "-i", "pipe:0", "-c", "copy",
             "-output_ts_offset", f"{offset_s:.9f}",
             "-muxdelay", "0", "-muxpreload", "0",
             "-f", "mpegts", "pipe:1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        )
        data = b"".join(client.fetch_object(seg))
        out, _ = proc.communicate(data)
        if proc.returncode != 0:
            raise RuntimeError(f"remux failed for {seg['object_id']}")
        sink.write(out)
        return len(out)


def consume_closed(
    client: TamsClient, flow_id: str, timerange: str, sink, remux: bool = False
) -> tuple[int, int]:
    """Export: as-fast-as-sink. Returns (segments, bytes)."""
    remuxer = Remuxer() if remux else None
    segs = 0
    total = 0
    for seg in client.iter_segments(flow_id, timerange):
        if remuxer:
            total += remuxer.write_segment(client, seg, sink)
        else:
            total += write_segment(client, seg, sink)
        segs += 1
    return segs, total


def consume_live(
    client: TamsClient,
    flow_id: str,
    timerange: str,
    sink,
    poll_s: float = 1.0,
    idle_timeout_s: float | None = None,
) -> tuple[int, int]:
    """Live: real-time paced, then follow the timeline as the recorder appends.

    Pacing is relative: the first served segment establishes the mapping between
    stream time and wall-clock; each later segment waits for its relative instant.
    Backpressure is the sink's own blocking write; we never buffer more than the
    segment being served.
    """
    rng = tl.timerange_to_ns(timerange)
    cursor_ns = rng.start_ns  # exclusive lower bound for the next query
    end_ns = rng.end_ns       # None for open-ended
    segs = 0
    total = 0
    t0_stream_ns: int | None = None
    t0_wall: float | None = None
    last_data = time.monotonic()

    while True:
        window = tl.ns_to_timerange(cursor_ns, end_ns)
        got = False
        for seg in client.iter_segments(flow_id, window):
            seg_rng = tl.timerange_to_ns(seg["timerange"])
            if cursor_ns is not None and seg_rng.end_ns <= cursor_ns:
                continue  # already served (overlap query is inclusive of partials)
            if t0_stream_ns is None:
                t0_stream_ns = seg_rng.start_ns
                t0_wall = time.monotonic()
            else:
                due = t0_wall + (seg_rng.start_ns - t0_stream_ns) / tl.NS
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            total += write_segment(client, seg, sink)
            segs += 1
            cursor_ns = seg_rng.end_ns
            got = True
            last_data = time.monotonic()
        if got:
            try:
                sink.flush()
            except Exception:
                pass
        if end_ns is not None and cursor_ns is not None and cursor_ns >= end_ns:
            break  # bounded live range fully served
        if idle_timeout_s is not None and time.monotonic() - last_data > idle_timeout_s:
            print(f"no new segments for {idle_timeout_s}s, closing", file=sys.stderr)
            break
        time.sleep(poll_s)
    return segs, total


def run_to_sink(args, sink) -> None:
    client = TamsClient(args.tams)
    rng = tl.timerange_to_ns(args.timerange)
    timerange = args.timerange
    # '_' (no bounds at all) means "the whole flow as it is now": resolve the
    # current extent and export it closed. Live mode is an explicit start with
    # an open end (e.g. '[1694429247:0_') or --live.
    if rng.start_ns is None and rng.end_ns is None and not args.live:
        timerange = client.get_flow(args.flow_id, include_timerange=True).get("timerange", "()")
        rng = tl.timerange_to_ns(timerange)
        if rng.start_ns is None or rng.end_ns is None:
            print("flow has no segments yet", file=sys.stderr)
            return
    live = args.live or rng.end_ns is None
    t0 = time.monotonic()
    if live:
        segs, total = consume_live(
            client, args.flow_id, timerange, sink,
            poll_s=args.poll, idle_timeout_s=args.until_idle,
        )
    else:
        segs, total = consume_closed(client, args.flow_id, timerange, sink, remux=args.remux)
    dt = time.monotonic() - t0
    mb = total / 1e6
    print(
        f"served {segs} segments, {mb:.1f} MB in {dt:.2f}s"
        + (f" ({mb / dt:.1f} MB/s)" if dt > 0 else ""),
        file=sys.stderr,
    )


def serve_http(args) -> None:
    """Minimal single-client HTTP sink: GET /stream.ts -> chunked mpegts."""
    import socket

    host, _, port = args.listen.rpartition(":")
    host = host or "0.0.0.0"
    srv = socket.create_server((host, int(port)))
    print(f"listening on http://{host}:{port}/stream.ts", file=sys.stderr)
    while True:
        conn, addr = srv.accept()
        print(f"client {addr[0]}:{addr[1]} connected", file=sys.stderr)
        try:
            conn.recv(4096)  # consume the request; single endpoint, no routing
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: video/mp2t\r\n"
                b"Cache-Control: no-store\r\n"
                b"Connection: close\r\n\r\n"
            )
            sink = conn.makefile("wb")
            run_to_sink(args, sink)
            sink.close()
        except (BrokenPipeError, ConnectionResetError):
            print("client disconnected", file=sys.stderr)
        finally:
            conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Consume a TAMS flow timerange to a sink")
    ap.add_argument("--tams", default="http://localhost:8000")
    ap.add_argument("--flow-id", required=True)
    ap.add_argument("--timerange", default="_", help="e.g. '[1694429247:0_1694429300:0)' or '[1694429247:0_' for live")
    ap.add_argument("--out", default=None, help="output file, or '-' for stdout")
    ap.add_argument("--listen", default=None, help="serve over HTTP instead, e.g. ':8090'")
    ap.add_argument("--remux", action="store_true",
                    help="rewrite mpegts timestamps to a continuous timeline (montages with discontinuities); -c copy, no re-encode")
    ap.add_argument("--live", action="store_true", help="force paced live mode even for closed ranges")
    ap.add_argument("--poll", type=float, default=1.0, help="follow-mode poll interval, seconds")
    ap.add_argument("--until-idle", type=float, default=None, help="stop live mode after N idle seconds")
    args = ap.parse_args()

    if args.listen:
        serve_http(args)
    elif args.out == "-" or args.out is None:
        run_to_sink(args, sys.stdout.buffer)
    else:
        with open(args.out, "wb") as f:
            run_to_sink(args, f)


if __name__ == "__main__":
    main()
