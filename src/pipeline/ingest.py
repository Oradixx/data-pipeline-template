"""Wire everything together: source -> batches -> sink, with a clean shutdown."""

import asyncio
import logging
import signal
import time
from typing import Any

import asyncpg

from pipeline.batching import batched
from pipeline.config import Settings
from pipeline.sink import TRANSIENT_ERRORS, Sink, TimescaleSink
from pipeline.sources import Source, SyntheticSensorSource

log = logging.getLogger(__name__)


async def run(
    source: Source, sink: Sink, stop: asyncio.Event, *, batch_size: int, max_wait_s: float
) -> int:
    """Move data until the source ends or ``stop`` is set. Returns the number of rows written."""
    total, started = 0, time.monotonic()
    async for batch in batched(source.stream(stop), batch_size, max_wait_s):
        await sink.write(batch)
        total += len(batch)
        log.info(
            "wrote %d rows (total %d, %.0f rows/s)",
            len(batch),
            total,
            total / max(time.monotonic() - started, 1e-9),
        )
    return total


async def create_pool(dsn: str, attempts: int = 10) -> Any:
    """Connect, waiting for the database if it is still starting up."""
    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(dsn, min_size=1, max_size=4)
        except TRANSIENT_ERRORS as exc:
            if attempt == attempts:
                raise
            log.warning("database not ready (%s), retry %d/%d", exc, attempt, attempts)
            await asyncio.sleep(min(2**attempt, 15))
    raise AssertionError("unreachable")


async def ingest(settings: Settings, *, ticks: int | None = None) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):  # Ctrl+C / `docker stop`
        loop.add_signal_handler(sig, stop.set)

    source = SyntheticSensorSource(settings.devices, settings.interval_s, ticks=ticks)
    pool = await create_pool(settings.database_url)
    try:
        total = await run(
            source,
            TimescaleSink(pool),
            stop,
            batch_size=settings.batch_size,
            max_wait_s=settings.batch_max_wait_s,
        )
    finally:
        await pool.close()
    log.info("stopped cleanly after %d rows", total)
    return total
