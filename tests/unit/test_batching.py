import asyncio
from collections.abc import AsyncIterator

import pytest

from pipeline.batching import batched


async def numbers(n: int, delay_s: float = 0.0) -> AsyncIterator[int]:
    for i in range(n):
        if delay_s:
            await asyncio.sleep(delay_s)
        yield i


async def collect(stream: AsyncIterator[list[int]]) -> list[list[int]]:
    return [batch async for batch in stream]


async def test_full_batches_then_remainder() -> None:
    batches = await collect(batched(numbers(7), max_size=3, max_wait_s=1.0))
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


async def test_slow_source_is_flushed_by_timeout() -> None:
    # One item every 50 ms, flush after 80 ms: batches stay small instead of waiting for 100 items.
    batches = await collect(batched(numbers(4, delay_s=0.05), max_size=100, max_wait_s=0.08))
    assert [x for b in batches for x in b] == [0, 1, 2, 3]
    assert len(batches) >= 2


async def test_empty_source_yields_nothing() -> None:
    assert await collect(batched(numbers(0), max_size=10, max_wait_s=0.1)) == []


async def test_source_error_is_propagated() -> None:
    async def broken() -> AsyncIterator[int]:
        yield 1
        raise RuntimeError("source crashed")

    with pytest.raises(RuntimeError, match="source crashed"):
        await collect(batched(broken(), max_size=10, max_wait_s=0.1))


async def test_invalid_size() -> None:
    with pytest.raises(ValueError):
        await collect(batched(numbers(1), max_size=0, max_wait_s=0.1))
