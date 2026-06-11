"""MinIO/S3 access for the TAMS store (`http_object_store` backend type).

Two endpoints are involved:
- MINIO_ENDPOINT: how the *store* reaches MinIO (inside compose: http://minio:9000)
- MINIO_PUBLIC_ENDPOINT: the host clients use; presigned URLs are signed against
  this endpoint (default http://localhost:9000). Presigning is offline, so the
  store never needs to connect to the public endpoint itself.
"""

from __future__ import annotations

import os

import boto3
from botocore.client import Config

BUCKET = os.environ.get("TAMS_BUCKET", "tams")
PRESIGN_TTL = int(os.environ.get("TAMS_PRESIGN_TTL", "3600"))


def _client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


class ObjectStore:
    def __init__(self) -> None:
        internal = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        public = os.environ.get("MINIO_PUBLIC_ENDPOINT", internal)
        self._internal = _client(internal)
        self._signer = _client(public)

    def ensure_bucket(self) -> None:
        try:
            self._internal.head_bucket(Bucket=BUCKET)
        except Exception:
            self._internal.create_bucket(Bucket=BUCKET)

    def presign_put(self, key: str) -> str:
        return self._signer.generate_presigned_url(
            "put_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=PRESIGN_TTL
        )

    def presign_get(self, key: str) -> str:
        return self._signer.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": key}, ExpiresIn=PRESIGN_TTL
        )

    def object_exists(self, key: str) -> bool:
        try:
            self._internal.head_object(Bucket=BUCKET, Key=key)
            return True
        except Exception:
            return False
