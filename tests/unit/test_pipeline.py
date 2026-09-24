"""Ingestion loop, sink retries and configuration — no database needed."""

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import pytest

from pipeline.config import Settings
from pipeline.ingest import run
from pipeline.models import Reading
from pipeline.sink import TimescaleSink
from pipeline.sources import SyntheticSensorSource


class MemorySink:
    def __init__(self) -> None:
        self.batches: list[Sequence[Reading]] = []

    async def write(self, batch: Sequence[Reading]) -> None:
        self.batches.append(batch)


async def test_run_moves_every_reading_to_the_sink() -> None:
    sink = MemorySink()
    source = SyntheticSensorSource(devices=2, interval_s=0, ticks=10, seed=0)
    total = await run(source, sink, asyncio.Event(), batch_size=7, max_wait_s=1)
    assert total == 2 * 3 * 10 == sum(len(b) for b in sink.batches)
    assert max(len(b) for b in sink.batches) <= 7


class FlakyPool:
    """Fails ``failures`` times with a connection error, then accepts writes."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.rows_written = 0

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[Any]:
        if self.failures:
            self.failures -= 1
            raise ConnectionResetError("database went away")
        yield self

    async def copy_records_to_table(self, table: str, *, records: Any, columns: Any) -> None:
        self.rows_written += len(records)


async def test_sink_retries_transient_errors() -> None:
    pool = FlakyPool(failures=2)
    batch = [Reading(datetime.now(UTC), "d", "m", 1.0)] * 3
    await TimescaleSink(pool, base_delay_s=0).write(batch)
    assert pool.rows_written == 3


async def test_sink_gives_up_after_max_retries() -> None:
    with pytest.raises(ConnectionResetError):
        await TimescaleSink(FlakyPool(failures=5), max_retries=3, base_delay_s=0).write([])


def test_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
    monkeypatch.setenv("BATCH_SIZE", "42")
    settings = Settings.from_env()
    assert settings.batch_size == 42
    assert settings.devices == 5


def test_settings_require_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        Settings.from_env()
