"""Write batches to TimescaleDB with PostgreSQL COPY, retrying transient failures."""

import asyncio
import logging
from collections.abc import Sequence
from typing import Any, Protocol

import asyncpg

from pipeline.models import COLUMNS, Reading

log = logging.getLogger(__name__)

# Errors worth retrying: network blips, database restarting. Anything else (bad data,
# missing table…) is a bug and should fail loudly.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.InterfaceError,
)


class Sink(Protocol):
    async def write(self, batch: Sequence[Reading]) -> None: ...


class TimescaleSink:
    def __init__(
        self,
        pool: Any,
        table: str = "sensor_readings",
        *,
        max_retries: int = 5,
        base_delay_s: float = 0.5,
    ) -> None:
        self.pool = pool
        self.table = table
        self.max_retries = max_retries
        self.base_delay_s = base_delay_s

    async def write(self, batch: Sequence[Reading]) -> None:
        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.pool.acquire() as conn:
                    # COPY is PostgreSQL's bulk-load path: far faster than INSERTs.
                    await conn.copy_records_to_table(self.table, records=batch, columns=COLUMNS)
                return
            except TRANSIENT_ERRORS as exc:
                if attempt == self.max_retries:
                    raise
                delay = min(self.base_delay_s * 2 ** (attempt - 1), 30.0)
                log.warning(
                    "write failed (%s), retry %d/%d in %.1fs",
                    exc,
                    attempt,
                    self.max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
