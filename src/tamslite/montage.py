"""tams-montage: build a new Flow by *reference* from clips of existing Flows.

This is the point of TAMS: an edit is metadata. The new Flow's segments point
at the same immutable media objects (same object_id, zero bytes copied); only
`timerange` and `ts_offset` change to re-position the media on the new timeline.

Clip boundaries snap to segment boundaries (chunk granularity): frame-accurate
trims would need `object_timerange` sub-ranges honoured by the player/remuxer,
which is out of scope for the pilot.

Usage:
  tams-montage --src-flow-id A --clip "[t1_t2)" --clip "[t3_t4)" [--src-flow-id B --clip ...]
Each --clip applies to the most recent --src-flow-id, so you can interleave
clips from different source flows (a montage that follows the switches).
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from . import timeline as tl
from .client import TamsClient


def collect_clip_segments(client: TamsClient, flow_id: str, clip_tr: str) -> list[dict]:
    segs = list(client.iter_segments(flow_id, clip_tr))
    if not segs:
        raise SystemExit(f"no segments in {flow_id} for {clip_tr}")
    return segs


def main() -> None:
    ap = argparse.ArgumentParser(description="Create a montage flow by reference (zero copy)")
    ap.add_argument("--tams", default="http://localhost:8000")
    ap.add_argument("--src-flow-id", action="append", required=True, dest="src_flows")
    ap.add_argument("--clip", action="append", required=True, metavar="TIMERANGE",
                    help="clip timerange on the source flow's timeline; repeatable")
    ap.add_argument("--flow-id", default=None, help="id for the montage flow")
    ap.add_argument("--label", default="tams-lite montage")
    ap.add_argument("--start", default="0:0", help="TAI start of the montage timeline")
    args = ap.parse_args()

    # pair clips with source flows: argparse keeps both lists in order; clips
    # belong to the most recent --src-flow-id on the command line. With a single
    # source flow, all clips belong to it.
    if len(args.src_flows) == 1:
        plan = [(args.src_flows[0], c) for c in args.clip]
    elif len(args.src_flows) == len(args.clip):
        plan = list(zip(args.src_flows, args.clip))
    else:
        raise SystemExit("use either one --src-flow-id, or one per --clip")

    client = TamsClient(args.tams)
    montage_flow_id = args.flow_id or str(uuid.uuid4())
    montage_source_id = str(uuid.uuid4())

    t_start = time.perf_counter()

    # the montage flow copies the essence of the first source flow
    src_doc = client.get_flow(plan[0][0])
    flow_doc = {
        "id": montage_flow_id,
        "source_id": montage_source_id,
        "label": args.label,
        "format": src_doc["format"],
        "codec": src_doc["codec"],
        "container": src_doc.get("container", "video/mp2t"),
        "essence_parameters": src_doc["essence_parameters"],
        "tags": {"montage_of": ",".join(dict.fromkeys(f for f, _ in plan))},
    }
    client.put_flow(flow_doc)

    out_pos_ns = tl.ts_to_ns(args.start)
    new_segments: list[dict] = []
    referenced_bytes_segments = 0
    for src_flow, clip_tr in plan:
        for seg in collect_clip_segments(client, src_flow, clip_tr):
            seg_rng = tl.timerange_to_ns(seg["timerange"])
            # snap to whole segments: shift the source segment onto the montage timeline
            new_start = out_pos_ns
            new_end = out_pos_ns + seg_rng.duration_ns()
            old_offset = tl.ts_to_ns(seg.get("ts_offset", "0:0"))
            # segment_ts moved by (new_start - old_start); media_ts unchanged
            new_offset = old_offset + (new_start - seg_rng.start_ns)
            new_segments.append({
                "object_id": seg["object_id"],          # same immutable object: zero copy
                "timerange": tl.ns_to_timerange(new_start, new_end),
                "ts_offset": tl.ns_to_ts(new_offset),
            })
            out_pos_ns = new_end
            referenced_bytes_segments += 1

    client.post_segments(montage_flow_id, new_segments)
    elapsed_ms = (time.perf_counter() - t_start) * 1000

    total_tr = tl.ns_to_timerange(tl.ts_to_ns(args.start), out_pos_ns)
    print(f"montage flow: {montage_flow_id}", file=sys.stderr)
    print(f"timeline:     {total_tr}", file=sys.stderr)
    print(
        f"created by reference in {elapsed_ms:.1f} ms — "
        f"{len(new_segments)} segments re-referenced, 0 media bytes copied",
        file=sys.stderr,
    )
    print(montage_flow_id)


if __name__ == "__main__":
    main()
