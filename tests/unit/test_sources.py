import asyncio

import pytest

from pipeline.models import Reading
from pipeline.sources import DEFAULT_METRICS, SyntheticSensorSource


async def take_all(
    source: SyntheticSensorSource, stop: asyncio.Event | None = None
) -> list[Reading]:
    return [r async for r in source.stream(stop or asyncio.Event())]


async def test_one_reading_per_device_metric_and_tick() -> None:
    readings = await take_all(SyntheticSensorSource(devices=3, interval_s=0, ticks=4, seed=1))
    assert len(readings) == 3 * len(DEFAULT_METRICS) * 4
    assert {r.device_id for r in readings} == {"device-001", "device-002", "device-003"}


async def test_same_seed_same_values() -> None:
    a = await take_all(SyntheticSensorSource(devices=2, interval_s=0, ticks=5, seed=42))
    b = await take_all(SyntheticSensorSource(devices=2, interval_s=0, ticks=5, seed=42))
    assert [r.value for r in a] == [r.value for r in b]


async def test_values_stay_around_their_mean() -> None:
    source = SyntheticSensorSource(devices=1, interval_s=0, ticks=500, anomaly_rate=0, seed=3)
    temps = [r.value for r in await take_all(source) if r.metric == "engine_temp_c"]
    assert 75 < sum(temps) / len(temps) < 95


async def test_stops_when_event_is_set() -> None:
    stop = asyncio.Event()
    source = SyntheticSensorSource(devices=1, interval_s=10)  # would block 10 s without stop
    stop.set()
    assert await asyncio.wait_for(take_all(source, stop), timeout=1) == []


def test_needs_at_least_one_device() -> None:
    with pytest.raises(ValueError):
        SyntheticSensorSource(devices=0)
