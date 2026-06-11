"""Shared TAMS client used by the recorder, consumer and montage tools.

Only speaks the public TAMS v8.1 API, so it works against any conformant
store, not just tams-lite.
"""

from __future__ import annotations

import json
from collections.abc import Iterator

import httpx


class TamsError(RuntimeError):
    pass


class TamsClient:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = httpx.Client(timeout=timeout)

    # ------------------------------------------------------------------ flows

    def put_flow(self, flow: dict) -> None:
        r = self.http.put(f"{self.base_url}/flows/{flow['id']}", json=flow)
        if r.status_code not in (200, 201, 204):
            raise TamsError(f"PUT flow failed: {r.status_code} {r.text}")

    def get_flow(self, flow_id: str, include_timerange: bool = False) -> dict:
        r = self.http.get(
            f"{self.base_url}/flows/{flow_id}",
            params={"include_timerange": str(include_timerange).lower()},
        )
        if r.status_code != 200:
            raise TamsError(f"GET flow failed: {r.status_code} {r.text}")
        return r.json()

    # ---------------------------------------------------------------- storage

    def allocate_storage(self, flow_id: str, limit: int) -> list[dict]:
        """Returns [{object_id, put_url:{url,...}}, ...]"""
        r = self.http.post(f"{self.base_url}/flows/{flow_id}/storage", json={"limit": limit})
        if r.status_code != 201:
            raise TamsError(f"POST storage failed: {r.status_code} {r.text}")
        return r.json()["media_objects"]

    def upload_object(self, put_url: dict, data: bytes) -> None:
        headers = dict(put_url.get("headers") or {})
        if put_url.get("content-type"):
            headers["Content-Type"] = put_url["content-type"]
        r = self.http.put(put_url["url"], content=data, headers=headers)
        if r.status_code not in (200, 201, 204):
            raise TamsError(f"object upload failed: {r.status_code} {r.text}")

    # --------------------------------------------------------------- segments

    def post_segments(self, flow_id: str, segments: dict | list[dict]) -> None:
        r = self.http.post(f"{self.base_url}/flows/{flow_id}/segments", json=segments)
        if r.status_code == 201:
            return
        if r.status_code == 200:
            failed = r.json().get("failed_segments", [])
            raise TamsError(f"{len(failed)} segment(s) failed: " + json.dumps(failed[:3]))
        raise TamsError(f"POST segments failed: {r.status_code} {r.text}")

    def iter_segments(
        self,
        flow_id: str,
        timerange: str = "_",
        limit: int = 200,
        include_object_timerange: bool = True,
    ) -> Iterator[dict]:
        """Yield segments in timeline order, following cursor pagination."""
        params: dict = {
            "timerange": timerange,
            "limit": limit,
            "include_object_timerange": str(include_object_timerange).lower(),
        }
        while True:
            r = self.http.get(f"{self.base_url}/flows/{flow_id}/segments", params=params)
            if r.status_code != 200:
                raise TamsError(f"GET segments failed: {r.status_code} {r.text}")
            yield from r.json()
            next_key = r.headers.get("X-Paging-NextKey")
            if not next_key:
                return
            params["page"] = next_key

    def fetch_object(self, segment: dict) -> Iterator[bytes]:
        """Stream the media object bytes of a segment from its first get_url."""
        urls = segment.get("get_urls") or []
        if not urls:
            raise TamsError(f"segment {segment.get('object_id')} has no get_urls")
        with self.http.stream("GET", urls[0]["url"]) as r:
            if r.status_code != 200:
                raise TamsError(f"object fetch failed: {r.status_code}")
            yield from r.iter_bytes(chunk_size=256 * 1024)
