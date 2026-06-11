# tams-lite

Lightweight, self-hostable implementation of the [BBC TAMS](https://github.com/bbc/tams)
(Time Addressable Media Store, **API v8.1**) record → export/live cycle.
**No Kubernetes**: one `docker-compose.yml` (FastAPI + Postgres + MinIO) on a modest host.

Three components:

| component | what it does |
|---|---|
| **store** (`tams-store`) | the minimal TAMS v8.1 API subset: `PUT /flows/{id}`, `POST /flows/{id}/storage`, `POST`/`GET /flows/{id}/segments` with timerange + cursor paging. Media bytes live in MinIO; TAMS holds only time-addressable metadata. |
| **recorder** (`tams-record`) | ingests a source (file, URL, or `testsrc`), segments it into immutable ~2s mpegts chunks with ffmpeg, uploads them via presigned PUT and registers Flow Segments on the TAI timeline. |
| **consumer** (`tams-consume`) | the streaming resolver: given a Flow + timerange it reads segments from TAMS and pours mpegts bytes to a sink — file, stdout pipe, or HTTP — **without ever copying media into new storage** (edit-by-reference). |

Plus `tams-montage`: builds a new Flow *by reference* from clips of existing Flows —
same immutable `object_id`s, new `timerange`/`ts_offset`. An edit is metadata.

## Why

Traditional export merges N files into one huge file: duplicated storage, heavy I/O, slow.
TAMS makes the edit a metadata operation over immutable, shareable segments. Measured here
(14s test recording, 3-clip montage):

| operation | time | media bytes moved |
|---|---|---|
| create montage flow (by reference) | **34 ms** | **0** |
| materialise same montage to .ts | 120 ms (with remux) | 1.6 MB |
| export full 14s flow to .ts | 60 ms (57 MB/s) | 3.7 MB |

The montage exists, is playable and shareable, before any byte moves. Materialisation —
if you need it at all — happens downstream, on the fly.

## Quickstart

```bash
docker compose up -d            # postgres + minio + tams-api on :8000

# 1. RECORD: 14s of test video into TAMS (2s chunks)
tams-record --source testsrc --duration 14 --flow-id <FLOW>

# 2. EXPORT: materialise the whole flow to a continuous .ts (read only from TAMS)
tams-consume --flow-id <FLOW> --out export.ts

# 3. MONTAGE by reference (zero copy), then materialise it
MONTAGE=$(tams-montage --src-flow-id <FLOW> --clip "[t1_t2)" --clip "[t3_t4)")
tams-consume --flow-id "$MONTAGE" --remux --out montage.ts

# 4. LIVE: open timerange = real-time paced stream that follows the recorder
tams-record --source testsrc --duration 60 &          # keeps recording...
tams-consume --flow-id <FLOW> --timerange "[<start>_" --out - | ffplay -   # ...while you watch
```

Install the CLIs with `pip install -e .` (Python ≥ 3.11; ffmpeg required for
recorder and `--remux`). Full prerequisites, install paths and configuration
are in [INSTALL.md](INSTALL.md).

## Semantics

The **timerange decides the mode** (one code path, different sink/pacing/termination):

- `[a_b)` closed → export: as fast as the sink accepts, then EOF.
- `[a_` open end → live: serves what exists real-time paced, then *follows* the
  timeline polling for new segments ("tail -f" of the flow). Ends on client
  disconnect, `--until-idle N`, or never.
- `_` (no bounds) → "the whole flow as it is now": resolved to the current extent,
  exported closed.

Sinks: `--out file`, `--out -` (stdout: pipe to ffplay/vlc/ffmpeg/socat — canonical
non-file transport, natural backpressure), `--listen :8090` (minimal single-client
HTTP `GET /stream.ts`, what players expect for live).

`--remux` rewrites mpegts timestamps onto a continuous timeline at discontinuity
boundaries (montages across re-referenced objects) with `ffmpeg -c copy` — no re-encode.

## Conformance

- Flow / Source / Flow Segment / storage payloads are validated against the
  **official v8.1 JSON Schemas**, vendored verbatim in `vendor/tams-schemas-8.1/`.
- Segment non-overlap (a spec MUST) is enforced by Postgres itself:
  `EXCLUDE USING gist (flow_id WITH =, ts_range WITH &&)` over int8range TAI nanoseconds.
- TAI timestamps/timeranges handled by BBC's own
  [`mediatimestamp`](https://github.com/bbc/rd-apmm-python-lib-mediatimestamp) library.
- Sources are created implicitly by `PUT /flows/{id}` (the spec has no `PUT /sources`).
- `tests/conformance/` validates both the official spec examples and the live
  responses of this store against the schemas. Run `pytest`.

Out of scope (pilot): deletion, webhooks/events, multi-essence flow collections,
auth, UI, clustering. The data model is interoperable with other TAMS
implementations (TAMOSS, AWS Labs reference).

## Layout

```
src/tamslite/
  store/        FastAPI store (app, schema.sql, validation, s3)
  recorder/     ffmpeg segmenter -> presigned upload -> segment registration
  consumer/     resolver: timerange -> mpegts sink (file / stdout / http)
  montage.py    edit-by-reference flow builder
  timeline.py   TAI timerange <-> int8 ns (mediatimestamp)
  client.py     TAMS API client (works against any conformant store)
vendor/         official v8.1 schemas + examples (tag 8.1, verbatim)
tests/          unit + conformance (schemas, live store)
```

## License

Apache-2.0 (see [LICENSE](LICENSE)). Copyright 2026 proschinesi.

Derived from the BBC R&D AMWA TAMS project; the vendored API schemas and examples
are © British Broadcasting Corporation, Apache-2.0, taken verbatim from
[bbc/tams](https://github.com/bbc/tams) tag `8.1`. See [NOTICE](NOTICE) for
attribution details.
