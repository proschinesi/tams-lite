"""asyncpg pool + schema bootstrap for the TAMS store."""

from __future__ import annotations

import json
import os
from pathlib import Path

import asyncpg

SCHEMA_SQL = (Path(__file__).parent / "schema.sql").read_text()


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql://tams:tams@localhost:5432/tams"
    )


async def create_pool() -> asyncpg.Pool:
    async def _init(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    pool = await asyncpg.create_pool(database_url(), init=_init, min_size=1, max_size=10)
    async with pool.acquire() as conn:
        # serialise concurrent bootstraps (CREATE EXTENSION IF NOT EXISTS can
        # still race at the catalog level)
        await conn.execute("SELECT pg_advisory_lock(7426315)")
        try:
            await conn.execute(SCHEMA_SQL)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7426315)")
    return pool
