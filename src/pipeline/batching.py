"""Group a stream into batches: flush when a batch is full OR when it has waited too long.

Writing row by row is slow (one network round trip per row); writing only full batches
means a quiet source may never flush. Size + timeout gives both throughput and freshness.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator


class _Done:
    """Sentinel put on the queue when the source is exhausted."""


_DONE = _Done()


async def batched[T](
    items: AsyncIterator[T], max_size: int, max_wait_s: float
) -> AsyncIterator[list[T]]:
    """Yield lists of at most ``max_size`` items, never holding an item longer than ``max_wait_s``.

    The bounded queue gives back-pressure: if the database is slow, the source is paused
    instead of filling memory. Errors raised by the source are re-raised to the caller.
    """
    if max_size < 1:
        raise ValueError("max_size must be >= 1")

    queue: asyncio.Queue[T | _Done] = asyncio.Queue(maxsize=max_size * 4)
    loop = asyncio.get_running_loop()

    async def pump() -> None:
        try:
            async for item in items:
                await queue.put(item)
        finally:
            await queue.put(_DONE)

    producer = asyncio.create_task(pump())
    try:
        exhausted = False
        while not exhausted:
            first = await queue.get()
            if isinstance(first, _Done):
                break
            batch = [first]
            deadline = loop.time() + max_wait_s
            while len(batch) < max_size:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if isinstance(item, _Done):
                    exhausted = True
                    break
                batch.append(item)
            yield batch
        await producer  # re-raise a source error, if any
    finally:
        if not producer.done():
            producer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await producer
