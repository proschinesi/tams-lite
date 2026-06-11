"""Validate live store responses against the official v8.1 schemas.

Needs the stack running (docker compose up / uvicorn + postgres + minio);
skipped otherwise. Creates its own throwaway flow.
"""

import uuid

import httpx
import pytest

from tamslite.store import validation

TAMS = "http://localhost:8000"


@pytest.fixture(scope="module")
def http():
    client = httpx.Client(base_url=TAMS, timeout=10)
    try:
        client.get("/service")
    except httpx.ConnectError:
        pytest.skip("store not running on :8000")
    return client


@pytest.fixture(scope="module")
def flow_id(http):
    fid = str(uuid.uuid4())
    flow = {
        "id": fid,
        "source_id": str(uuid.uuid4()),
        "format": "urn:x-nmos:format:video",
        "codec": "video/h264",
        "container": "video/mp2t",
        "label": "conformance test",
        "essence_parameters": {
            "frame_width": 640,
            "frame_height": 360,
            "frame_rate": {"numerator": 25},
        },
    }
    r = http.put(f"/flows/{fid}", json=flow)
    assert r.status_code == 201, r.text
    assert validation.validate("flow.json", r.json()) == []
    return fid


def test_flow_get_validates(http, flow_id):
    r = http.get(f"/flows/{flow_id}", params={"include_timerange": "true"})
    assert r.status_code == 200
    assert validation.validate("flow.json", r.json()) == []


def test_source_get_validates(http, flow_id):
    source_id = http.get(f"/flows/{flow_id}").json()["source_id"]
    r = http.get(f"/sources/{source_id}")
    assert r.status_code == 200
    assert validation.validate("source.json", r.json()) == []


def test_storage_and_segment_cycle_validates(http, flow_id):
    r = http.post(f"/flows/{flow_id}/storage", json={"limit": 2})
    assert r.status_code == 201, r.text
    storage = r.json()
    assert validation.validate("flow-storage.json", storage) == []

    obj = storage["media_objects"][0]
    up = httpx.put(obj["put_url"]["url"], content=b"\x47" * 188 * 16)
    assert up.status_code in (200, 204), up.text

    seg = {"object_id": obj["object_id"], "timerange": "[100:0_102:0)", "ts_offset": "98:520000000"}
    r = http.post(f"/flows/{flow_id}/segments", json=seg)
    assert r.status_code == 201, r.text

    r = http.get(f"/flows/{flow_id}/segments", params={"timerange": "[99:0_103:0)"})
    assert r.status_code == 200
    items = r.json()
    assert len(items) == 1
    for item in items:
        assert validation.validate("flow-segment.json", item) == []
    assert r.headers["X-Paging-Count"] == "1"

    # the registered object must be retrievable via its presigned get_url
    got = httpx.get(items[0]["get_urls"][0]["url"])
    assert got.status_code == 200 and got.content[:1] == b"\x47"


def test_overlap_rejected_as_partial_failure(http, flow_id):
    r = http.post(f"/flows/{flow_id}/storage", json={"limit": 1})
    obj = r.json()["media_objects"][0]
    httpx.put(obj["put_url"]["url"], content=b"\x47" * 188)
    seg = {"object_id": obj["object_id"], "timerange": "[101:0_103:0)"}  # overlaps [100:0_102:0)
    r = http.post(f"/flows/{flow_id}/segments", json=seg)
    assert r.status_code == 200
    body = r.json()
    assert validation.validate("flow-segment-bulk-failure.json", body) == []
    assert len(body["failed_segments"]) == 1


def test_segments_paging(http, flow_id):
    r = http.post(f"/flows/{flow_id}/storage", json={"limit": 3})
    for i, obj in enumerate(r.json()["media_objects"]):
        httpx.put(obj["put_url"]["url"], content=b"\x47" * 188)
        s = http.post(
            f"/flows/{flow_id}/segments",
            json={"object_id": obj["object_id"], "timerange": f"[{200 + 2 * i}:0_{202 + 2 * i}:0)"},
        )
        assert s.status_code == 201
    r = http.get(f"/flows/{flow_id}/segments", params={"timerange": "[200:0_206:0)", "limit": 2})
    assert r.headers["X-Paging-Count"] == "2"
    next_key = r.headers.get("X-Paging-NextKey")
    assert next_key and "Link" in r.headers
    r2 = http.get(
        f"/flows/{flow_id}/segments",
        params={"timerange": "[200:0_206:0)", "limit": 2, "page": next_key},
    )
    assert [s["timerange"] for s in r2.json()] == ["[204:0_206:0)"]
