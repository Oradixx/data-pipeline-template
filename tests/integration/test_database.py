"""End-to-end tests against a real TimescaleDB.

Run locally with `make up` then `make test-integration`; CI starts TimescaleDB as a service.
Skipped automatically when DATABASE_URL is not set.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from pipeline.ingest import run
from pipeline.migrate import apply, discover
from pipeline.sink import TimescaleSink
from pipeline.sources import SyntheticSensorSource

pytestmark = pytest.mark.integration

DATABASE_URL = os.getenv("DATABASE_URL")
MIGRATIONS = Path(__file__).parents[2] / "db" / "migrations"

if not DATABASE_URL:
    pytest.skip("DATABASE_URL not set", allow_module_level=True)


@pytest.fixture
async def conn() -> AsyncIterator[Any]:
    connection = await asyncpg.connect(DATABASE_URL)
    await apply(connection, discover(MIGRATIONS))
    await connection.execute("TRUNCATE sensor_readings")
    yield connection
    await connection.close()


async def test_migrations_are_idempotent(conn: Any) -> None:
    assert await apply(conn, discover(MIGRATIONS)) == []  # everything already applied
    is_hypertable = await conn.fetchval(
        "SELECT count(*) FROM timescaledb_information.hypertables "
        "WHERE hypertable_name = 'sensor_readings'"
    )
    assert is_hypertable == 1


async def test_ingested_rows_land_in_table_and_aggregate(conn: Any) -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    try:
        source = SyntheticSensorSource(devices=2, interval_s=0, ticks=20, seed=1)
        written = await run(
            source, TimescaleSink(pool), asyncio.Event(), batch_size=50, max_wait_s=0.5
        )
    finally:
        await pool.close()

    assert written == 2 * 3 * 20
    assert await conn.fetchval("SELECT count(*) FROM sensor_readings") == written
    # Real-time aggregation: fresh rows are visible in the hourly view without a refresh.
    recent = await conn.fetchval(
        "SELECT sum(n_readings) FROM readings_hourly WHERE bucket > now() - INTERVAL '2 hours'"
    )
    assert recent == written
