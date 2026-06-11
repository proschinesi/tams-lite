"""Conformance: live store responses must validate against the official
BBC TAMS v8.1 JSON Schemas (vendored verbatim in vendor/tams-schemas-8.1).

Requires a running store with at least one recorded flow:
    docker compose up -d && tams-record --source testsrc --duration 6
Skipped automatically if no store is reachable.
"""

import uuid

import httpx
import pytest

from tamslite.store import validation

TAMS = "http://localhost:8000"


def _store_up() -> bool:
    try:
        return httpx.get(f"{TAMS}/service", timeout=2).status_code == 200
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _store_up(), reason="no TAMS store on :8000")


@pytest.fixture(scope="module")
def any_flow() -> dict:
    flows = httpx.get(f"{TAMS}/flows").json()
    if not flows:
        pytest.skip("store has no flows; run tams-record first")
    return flows[0]


def test_flow_validates_against_official_schema(any_flow):
    r = httpx.get(f"{TAMS}/flows/{any_flow['id']}", params={"include_timerange": "true"})
    assert r.status_code == 200
    assert validation.validate("flow.json", r.json()) == []


def test_source_validates_against_official_schema(any_flow):
    r = httpx.get(f"{TAMS}/sources/{any_flow['source_id']}")
    assert r.status_code == 200
    assert validation.validate("source.json", r.json()) == []


def test_segments_validate_against_official_schema(any_flow):
    r = httpx.get(f"{TAMS}/flows/{any_flow['id']}/segments", params={"limit": 5})
    assert r.status_code == 200
    segs = r.json()
    assert segs, "expected at least one segment"
    for seg in segs:
        assert validation.validate("flow-segment.json", seg) == []
    assert "X-Paging-Limit" in r.headers
    assert "X-Paging-Count" in r.headers


def test_segments_paging_walks_whole_timeline(any_flow):
    seen = []
    params = {"limit": 2}
    url = f"{TAMS}/flows/{any_flow['id']}/segments"
    while True:
        r = httpx.get(url, params=params)
        page = r.json()
        seen.extend(s["timerange"] for s in page)
        next_key = r.headers.get("X-Paging-NextKey")
        if not next_key:
            break
        params["page"] = next_key
    assert len(seen) == len(set(seen)), "paging returned duplicates"
    full = httpx.get(url, params={"limit": 1000}).json()
    assert len(seen) == len(full)


def test_storage_allocation_validates_and_put_url_works(any_flow):
    r = httpx.post(f"{TAMS}/flows/{any_flow['id']}/storage", json={"limit": 2})
    assert r.status_code == 201
    body = r.json()
    assert validation.validate("flow-storage.json", body) == []
    assert len(body["media_objects"]) == 2
    # presigned PUT must actually accept bytes
    put = body["media_objects"][0]["put_url"]
    up = httpx.put(put["url"], content=b"x" * 16,
                   headers={"Content-Type": put.get("content-type", "")})
    assert up.status_code in (200, 204)


def test_segment_overlap_rejected(any_flow):
    flow_id = any_flow["id"]
    segs = httpx.get(f"{TAMS}/flows/{flow_id}/segments", params={"limit": 1}).json()
    existing = segs[0]
    # shift by 1ns so it overlaps without being byte-identical (which would hit
    # the primary key, a different failure)
    from tamslite import timeline as tl

    rng = tl.timerange_to_ns(existing["timerange"])
    overlapping = tl.ns_to_timerange(rng.start_ns + 1, rng.end_ns + 1)
    alloc = httpx.post(f"{TAMS}/flows/{flow_id}/storage", json={"limit": 1}).json()
    r = httpx.post(
        f"{TAMS}/flows/{flow_id}/segments",
        json={"object_id": alloc["media_objects"][0]["object_id"],
              "timerange": overlapping},
    )
    assert r.status_code == 200  # partial-failure response
    body = r.json()
    assert validation.validate("flow-segment-bulk-failure.json", body) == []
    assert "overlap" in body["failed_segments"][0]["error"]["summary"]


def test_unknown_flow_segments_is_empty_list_not_404():
    r = httpx.get(f"{TAMS}/flows/{uuid.uuid4()}/segments")
    assert r.status_code == 200
    assert r.json() == []
