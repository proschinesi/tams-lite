"""tams-lite store: the minimal BBC TAMS v8.1 API subset.

Implemented (the record -> export/live cycle):
  GET  /service
  GET  /flows
  GET  /flows/{flowId}            (+ include_timerange)
  PUT  /flows/{flowId}            (auto-creates the Source; spec has no PUT /sources)
  POST /flows/{flowId}/storage    (presigned PUT urls for media objects)
  POST /flows/{flowId}/segments   (single or array; 201 all-ok / 200 partial failure)
  GET  /flows/{flowId}/segments   (timerange overlap + cursor paging headers)

Everything else in the spec (deletion, webhooks, tags subresources, multi-essence)
is intentionally out of scope for the pilot.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import uuid

import asyncpg
from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from .. import timeline as tl
from . import validation
from .db import create_pool
from .s3 import ObjectStore

STORAGE_LABEL = "tams-lite-minio"
# fixed id for the single backend (spec's uuid.json only admits versions 1-5)
STORAGE_ID = "0190e6a0-0000-4000-8000-000000000001"
MAX_LIMIT = 1000
DEFAULT_LIMIT = 200

# Fields the service manages; ignored if a client sends them (per spec SHOULD).
FLOW_MANAGED = {"created", "metadata_updated", "segments_updated", "timerange", "collected_by", "created_by", "updated_by"}


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _err(status: int, summary: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"type": "error", "summary": summary, "time": _now_iso()})


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state.store = ObjectStore()
    app.state.store.ensure_bucket()
    yield
    await app.state.pool.close()


app = FastAPI(title="tams-lite", version="0.1.0", lifespan=lifespan)


# --------------------------------------------------------------------------- service

@app.get("/service")
async def get_service():
    return {
        "name": "tams-lite",
        "description": "Lightweight self-hostable TAMS store (BBC TAMS API v8.1 subset)",
        "api_version": "8.1",
        "service_version": "tams-lite/0.1.0",
        "media_store": {"type": "http_object_store"},
        "event_stream_mechanisms": [],
    }


@app.get("/service/storage-backends")
async def get_storage_backends():
    return [
        {
            "id": STORAGE_ID,
            "label": STORAGE_LABEL,
            "default_storage": True,
            "store_type": "http_object_store",
            "provider": "minio",
            "store_product": "minio",
        }
    ]


# ----------------------------------------------------------------------------- flows

def _flow_response(row, timerange: str | None) -> dict:
    doc = dict(row["doc"])
    doc["created"] = row["created"].isoformat().replace("+00:00", "Z")
    doc["metadata_updated"] = row["metadata_updated"].isoformat().replace("+00:00", "Z")
    if row["segments_updated"]:
        doc["segments_updated"] = row["segments_updated"].isoformat().replace("+00:00", "Z")
    if timerange is not None:
        doc["timerange"] = timerange
    return doc


async def _flow_timerange(conn, flow_id: uuid.UUID, within: str | None = None) -> str:
    rng = tl.timerange_to_ns(within) if within else tl.RangeNS(None, None)
    row = await conn.fetchrow(
        """SELECT min(lower(ts_range)) AS lo, max(upper(ts_range)) AS hi
           FROM segments WHERE flow_id = $1
             AND ($2::bigint IS NULL OR upper(ts_range) > $2)
             AND ($3::bigint IS NULL OR lower(ts_range) < $3)""",
        flow_id, rng.start_ns, rng.end_ns,
    )
    if row is None or row["lo"] is None:
        return "()"
    return tl.ns_to_timerange(row["lo"], row["hi"])


@app.get("/flows")
async def list_flows(request: Request, source_id: uuid.UUID | None = None, format: str | None = None):
    rows = await request.app.state.pool.fetch(
        """SELECT * FROM flows
           WHERE ($1::uuid IS NULL OR source_id = $1)
             AND ($2::text IS NULL OR format = $2)
           ORDER BY created""",
        source_id, format,
    )
    return [_flow_response(r, None) for r in rows]


@app.get("/flows/{flow_id}")
async def get_flow(
    request: Request,
    flow_id: uuid.UUID,
    include_timerange: bool = False,
    timerange: str | None = None,
):
    pool = request.app.state.pool
    row = await pool.fetchrow("SELECT * FROM flows WHERE id = $1", flow_id)
    if row is None:
        return _err(404, "flow not found")
    tr = None
    if include_timerange:
        async with pool.acquire() as conn:
            tr = await _flow_timerange(conn, flow_id, timerange)
    return _flow_response(row, tr)


@app.put("/flows/{flow_id}")
async def put_flow(request: Request, flow_id: uuid.UUID):
    body = await request.json()
    errors = validation.validate("flow.json", body)
    if errors:
        return _err(400, "invalid flow: " + "; ".join(errors[:5]))
    if body["id"] != str(flow_id):
        return _err(400, "flow id in body does not match path")

    doc = {k: v for k, v in body.items() if k not in FLOW_MANAGED}
    source_id = uuid.UUID(body["source_id"])
    fmt = body["format"]

    pool = request.app.state.pool
    async with pool.acquire() as conn, conn.transaction():
        src = await conn.fetchrow("SELECT format FROM sources WHERE id = $1", source_id)
        if src is None:
            # Sources are created implicitly by Flow registration (no PUT /sources in spec)
            source_doc = {"id": str(source_id), "format": fmt}
            if "label" in body:
                source_doc["label"] = body["label"]
            await conn.execute(
                "INSERT INTO sources (id, format, doc) VALUES ($1, $2, $3)",
                source_id, fmt, source_doc,
            )
        elif src["format"] != fmt:
            return _err(400, f"flow format {fmt} conflicts with source format {src['format']}")

        existing = await conn.fetchval("SELECT 1 FROM flows WHERE id = $1", flow_id)
        await conn.execute(
            """INSERT INTO flows (id, source_id, format, codec, container, doc)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (id) DO UPDATE SET
                 source_id = EXCLUDED.source_id, format = EXCLUDED.format,
                 codec = EXCLUDED.codec, container = EXCLUDED.container,
                 doc = EXCLUDED.doc, metadata_updated = now()""",
            flow_id, source_id, fmt, body.get("codec"), body.get("container"), doc,
        )
        row = await conn.fetchrow("SELECT * FROM flows WHERE id = $1", flow_id)

    if existing:
        return Response(status_code=204)
    return JSONResponse(status_code=201, content=_flow_response(row, None))


# ------------------------------------------------------------------------- sources

@app.get("/sources")
async def list_sources(request: Request):
    rows = await request.app.state.pool.fetch("SELECT * FROM sources ORDER BY created")
    return [_source_response(r) for r in rows]


@app.get("/sources/{source_id}")
async def get_source(request: Request, source_id: uuid.UUID):
    row = await request.app.state.pool.fetchrow("SELECT * FROM sources WHERE id = $1", source_id)
    if row is None:
        return _err(404, "source not found")
    return _source_response(row)


def _source_response(row) -> dict:
    doc = dict(row["doc"])
    doc["created"] = row["created"].isoformat().replace("+00:00", "Z")
    doc["updated"] = row["updated"].isoformat().replace("+00:00", "Z")
    return doc


# ------------------------------------------------------------------------- storage

@app.post("/flows/{flow_id}/storage")
async def post_storage(request: Request, flow_id: uuid.UUID):
    body = await request.json() if (await request.body()) else {}
    errors = validation.validate("flow-storage-post.json", body)
    if errors:
        return _err(400, "invalid storage request: " + "; ".join(errors[:5]))

    pool = request.app.state.pool
    flow = await pool.fetchrow("SELECT container FROM flows WHERE id = $1", flow_id)
    if flow is None:
        return _err(404, "flow not found")
    if not flow["container"]:
        return _err(400, "flow 'container' is not set")

    if body.get("object_ids"):
        object_ids = body["object_ids"]
        dup = await pool.fetchval(
            "SELECT object_id FROM objects WHERE object_id = ANY($1) LIMIT 1", object_ids
        )
        if dup:
            return _err(400, f"object_id already exists: {dup}")
    else:
        n = min(int(body.get("limit") or 1), 100)
        object_ids = [f"{flow_id}/{uuid.uuid4().hex}" for _ in range(n)]

    store: ObjectStore = request.app.state.store
    media_objects = []
    async with pool.acquire() as conn, conn.transaction():
        for oid in object_ids:
            await conn.execute(
                """INSERT INTO objects (object_id, flow_id_created, storage_key)
                   VALUES ($1, $2, $3)""",
                oid, flow_id, oid,
            )
            media_objects.append({"object_id": oid, "put_url": {"url": store.presign_put(oid), "content-type": flow["container"]}})
    return JSONResponse(status_code=201, content={"media_objects": media_objects})


# ------------------------------------------------------------------------ segments

def _seg_failure(seg: dict, summary: str) -> dict:
    out = {"object_id": seg.get("object_id", ""), "error": {"type": "error", "summary": summary, "time": _now_iso()}}
    if "timerange" in seg:
        out["timerange"] = seg["timerange"]
    return out


@app.post("/flows/{flow_id}/segments")
async def post_segments(request: Request, flow_id: uuid.UUID):
    body = await request.json()
    segs = body if isinstance(body, list) else [body]

    pool = request.app.state.pool
    flow = await pool.fetchrow("SELECT id FROM flows WHERE id = $1", flow_id)
    if flow is None:
        return _err(404, "flow not found")

    failures: list[dict] = []
    async with pool.acquire() as conn:
        for seg in segs:
            errors = validation.validate("flow-segment-post.json", seg)
            if errors:
                failures.append(_seg_failure(seg, "; ".join(errors[:3])))
                continue
            try:
                rng = tl.timerange_to_ns(seg["timerange"])
            except Exception as e:  # malformed despite pattern (e.g. reversed)
                failures.append(_seg_failure(seg, f"bad timerange: {e}"))
                continue
            if not rng.bounded or rng.duration_ns() <= 0:
                failures.append(_seg_failure(seg, "segment timerange must be bounded and non-empty"))
                continue
            ts_offset_ns = tl.ts_to_ns(seg.get("ts_offset", "0:0"))

            async with conn.transaction():
                obj = await conn.fetchrow(
                    "SELECT * FROM objects WHERE object_id = $1 FOR UPDATE", seg["object_id"]
                )
                if obj is None:
                    failures.append(_seg_failure(seg, "unknown object_id (allocate via /flows/{id}/storage first)"))
                    continue
                in_use = await conn.fetchval(
                    "SELECT 1 FROM segments WHERE object_id = $1 LIMIT 1", seg["object_id"]
                )
                if not in_use and obj["flow_id_created"] != flow_id:
                    # spec: reject new objects that were not allocated for this flow
                    failures.append(_seg_failure(seg, "object was allocated for another flow and has no prior use"))
                    continue

                # object_timerange: first registration defaults to timerange - ts_offset
                obj_tr = seg.get("object_timerange")
                if in_use:
                    if obj_tr and obj["object_timerange"] and obj_tr != obj["object_timerange"]:
                        failures.append(_seg_failure(seg, "object_timerange conflicts with existing media object"))
                        continue
                    if seg.get("key_frame_count") is not None and obj["key_frame_count"] is not None \
                            and seg["key_frame_count"] != obj["key_frame_count"]:
                        failures.append(_seg_failure(seg, "key_frame_count conflicts with existing media object"))
                        continue
                else:
                    if not obj_tr:
                        obj_tr = tl.ns_to_timerange(rng.start_ns - ts_offset_ns, rng.end_ns - ts_offset_ns)
                    await conn.execute(
                        "UPDATE objects SET object_timerange = $2, key_frame_count = $3, uploaded = true WHERE object_id = $1",
                        seg["object_id"], obj_tr, seg.get("key_frame_count"),
                    )

                doc = {k: v for k, v in seg.items() if k != "get_urls"}
                try:
                    await conn.execute(
                        """INSERT INTO segments (flow_id, object_id, ts_range, doc)
                           VALUES ($1, $2, int8range($3, $4), $5)""",
                        flow_id, seg["object_id"], rng.start_ns, rng.end_ns, doc,
                    )
                except asyncpg.exceptions.ExclusionViolationError:
                    failures.append(_seg_failure(seg, "timerange overlaps an existing segment in this flow"))
                    continue
                except asyncpg.exceptions.UniqueViolationError:
                    failures.append(_seg_failure(seg, "segment already exists"))
                    continue

        await conn.execute("UPDATE flows SET segments_updated = now() WHERE id = $1", flow_id)

    if failures:
        return JSONResponse(status_code=200, content={"failed_segments": failures})
    return Response(status_code=201)


@app.get("/flows/{flow_id}/segments")
async def get_segments(
    request: Request,
    response: Response,
    flow_id: uuid.UUID,
    timerange: str = "_",
    object_id: str | None = None,
    reverse_order: bool = False,
    presigned: bool | None = None,
    include_object_timerange: bool = False,
    accept_get_urls: str | None = None,
    page: str | None = None,
    limit: int = Query(default=DEFAULT_LIMIT, ge=1),
):
    limit = min(limit, MAX_LIMIT)
    try:
        rng = tl.timerange_to_ns(timerange)
    except Exception:
        return _err(400, f"invalid timerange: {timerange}")
    cursor = None
    if page is not None:
        try:
            cursor = int(page)
        except ValueError:
            return _err(400, "invalid page cursor")

    order = "DESC" if reverse_order else "ASC"
    cmp = "<" if reverse_order else ">"
    rows = await request.app.state.pool.fetch(
        f"""SELECT s.*, o.storage_key, o.object_timerange AS obj_tr, o.key_frame_count AS obj_kfc
            FROM segments s JOIN objects o USING (object_id)
            WHERE s.flow_id = $1
              AND ($2::bigint IS NULL OR upper(s.ts_range) > $2)
              AND ($3::bigint IS NULL OR lower(s.ts_range) < $3)
              AND ($4::text IS NULL OR s.object_id = $4)
              AND ($5::bigint IS NULL OR lower(s.ts_range) {cmp} $5)
            ORDER BY lower(s.ts_range) {order}
            LIMIT $6""",
        flow_id, rng.start_ns, rng.end_ns, object_id, cursor, limit + 1,
    )

    has_more = len(rows) > limit
    rows = rows[:limit]
    store: ObjectStore = request.app.state.store

    want_urls = True
    if accept_get_urls is not None:
        labels = [l for l in accept_get_urls.split(",") if l]
        want_urls = STORAGE_LABEL in labels
    if presigned is False:
        want_urls = False  # our only urls are presigned

    items = []
    for r in rows:
        item = dict(r["doc"])
        if want_urls:
            item["get_urls"] = [{
                "url": store.presign_get(r["storage_key"]),
                "label": STORAGE_LABEL,
                "presigned": True,
                "storage_id": STORAGE_ID,
            }]
        if r["obj_kfc"] is not None and "key_frame_count" not in item:
            item["key_frame_count"] = r["obj_kfc"]
        if include_object_timerange and r["obj_tr"]:
            ts_off = tl.ts_to_ns(item.get("ts_offset", "0:0"))
            seg_rng = tl.timerange_to_ns(item["timerange"])
            derived = tl.ns_to_timerange(seg_rng.start_ns - ts_off, seg_rng.end_ns - ts_off)
            if r["obj_tr"] != derived:
                item["object_timerange"] = r["obj_tr"]
        items.append(item)

    response.headers["X-Paging-Limit"] = str(limit)
    response.headers["X-Paging-Count"] = str(len(items))
    response.headers["X-Paging-Reverse-Order"] = str(reverse_order).lower()
    if rows:
        los = [r["ts_range"].lower for r in rows]
        his = [r["ts_range"].upper for r in rows]
        response.headers["X-Paging-Timerange"] = tl.ns_to_timerange(min(los), max(his))
        if has_more:
            next_key = str(rows[-1]["ts_range"].lower)
            response.headers["X-Paging-NextKey"] = next_key
            q = dict(request.query_params)
            q["page"] = next_key
            q["limit"] = str(limit)
            url = str(request.url.replace_query_params(**q))
            response.headers["Link"] = f'<{url}>; rel="next"'
    return items


def main() -> None:
    import uvicorn

    uvicorn.run(
        "tamslite.store.app:app",
        host=os.environ.get("TAMS_HOST", "0.0.0.0"),
        port=int(os.environ.get("TAMS_PORT", "8000")),
    )


if __name__ == "__main__":
    main()
