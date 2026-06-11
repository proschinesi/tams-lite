# Installation & Dependencies

tams-lite runs as three components (store, recorder, consumer) plus a montage
tool, all from a single Python package, backed by Postgres + MinIO. **No
Kubernetes** — everything runs via `docker-compose` or directly on a host.

## Prerequisites

| Requirement | Why | Notes |
|---|---|---|
| **Docker** + Docker Compose v2 | runs Postgres, MinIO and the store | only this is needed to bring the store up |
| **ffmpeg / ffprobe** on `PATH` | recorder segments media; `--remux` rewrites mpegts timestamps | install separately (`brew install ffmpeg`, `apt install ffmpeg`) |
| **Python ≥ 3.11** | the `tams-record` / `tams-consume` / `tams-montage` CLIs | only needed if you run the CLIs on the host (not required if you only run the store in Docker) |

Postgres needs the `btree_gist` extension — already present in the official
`postgres:16` image used by compose; the store creates it on first start.

## Python dependencies

Declared in `pyproject.toml` (installed automatically by the steps below):

- `fastapi`, `uvicorn[standard]` — the store HTTP API
- `asyncpg` — Postgres driver
- `boto3` — S3/MinIO presigned URLs
- `httpx` — TAMS API client (recorder/consumer/montage)
- `jsonschema` — validation against the vendored v8.1 schemas
- `mediatimestamp` — BBC's TAI timestamp/timerange library

Dev extras (`pip install -e ".[dev]"`): `pytest`, `pytest-asyncio`.

## Option A — full stack via Docker Compose (recommended)

Brings up Postgres, MinIO and the store on `:8000`:

```bash
docker compose up -d        # postgres:5432, minio:9000 (console :9001), tams-api:8000
curl -s localhost:8000/service   # -> {"api_version":"8.1",...}
```

To run the **recorder/consumer/montage CLIs** you still install the Python
package on the host (they talk to the store over HTTP):

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Option B — run the store on the host (no Docker for the API)

Useful for development. You still need Postgres + MinIO (start just those two
with `docker compose up -d postgres minio`, or point at your own):

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

export DATABASE_URL=postgresql://tams:tams@localhost:5432/tams
export MINIO_ENDPOINT=http://localhost:9000
tams-store        # uvicorn on 0.0.0.0:8000
```

## Configuration (environment variables)

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `DATABASE_URL` | `postgresql://tams:tams@localhost:5432/tams` | store | Postgres connection string |
| `MINIO_ENDPOINT` | `http://localhost:9000` | store | how the store reaches MinIO (in compose: `http://minio:9000`) |
| `MINIO_PUBLIC_ENDPOINT` | = `MINIO_ENDPOINT` | store | host clients use; presigned URLs are signed for this endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | store | MinIO/S3 access key |
| `MINIO_SECRET_KEY` | `minioadmin` | store | MinIO/S3 secret key |
| `TAMS_BUCKET` | `tams` | store | object-store bucket for media |
| `TAMS_PRESIGN_TTL` | `3600` | store | presigned URL lifetime, seconds |
| `TAMS_HOST` / `TAMS_PORT` | `0.0.0.0` / `8000` | store | bind address/port |
| `TAMS_SCHEMA_DIR` | `vendor/tams-schemas-8.1` (auto) | store | override only if the vendored schemas move |

> **Presigned URL gotcha:** if clients run outside the Docker network, set
> `MINIO_PUBLIC_ENDPOINT` to the host MinIO can be reached at (e.g.
> `http://localhost:9000`), otherwise the URLs sign for `minio:9000` and fail
> from the host. Compose already wires this via the `MINIO_PUBLIC_ENDPOINT`
> build arg / env (default `http://localhost:9000`).

## Verify the install

```bash
# unit tests (no services needed)
pytest tests/test_timeline.py -q

# full suite incl. live conformance (needs the stack up)
docker compose up -d
pytest -q                       # 25 passed

# end-to-end smoke: record -> export -> montage -> live
./scripts/demo.sh
```

## Common issues

- **`tams-record` fails with "ffmpeg: command not found"** — install ffmpeg; it's
  not a Python dependency.
- **Object upload / fetch 403 from the host** — `MINIO_PUBLIC_ENDPOINT` mismatch
  (see the gotcha above).
- **Store won't start, `btree_gist` error** — using a Postgres image without the
  extension; stick to `postgres:16` (the compose default).
- **`pip install` resolves nothing to run** — activate the venv first; the CLIs
  are entry points installed by `pip install -e .`.
