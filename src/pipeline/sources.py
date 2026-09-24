"""Data sources. Anything with an async ``stream(stop)`` method can feed the pipeline.

The synthetic source simulates engine telemetry so the whole stack runs without any
external data. Replace it with a real source (S3 files, an API, a websocket, a message
queue…) by implementing the same ``Source`` protocol.
"""

import asyncio
import contextlib
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pipeline.models import Reading


class Source(Protocol):
    def stream(self, stop: asyncio.Event) -> AsyncIterator[Reading]:
        """Yield readings until ``stop`` is set (or the source is exhausted)."""
        ...


@dataclass(frozen=True, slots=True)
class MetricSpec:
    name: str
    mean: float
    noise: float
    reversion: float = 0.1  # how fast the value is pulled back to its mean (0..1)


DEFAULT_METRICS = (
    MetricSpec("engine_temp_c", mean=85.0, noise=0.8),
    MetricSpec("oil_pressure_bar", mean=4.2, noise=0.05),
    MetricSpec("rpm", mean=1500.0, noise=25.0),
)


class SyntheticSensorSource:
    """Mean-reverting random walks, one per (device, metric), with rare spikes.

    Args:
        devices: number of simulated devices.
        interval_s: pause between two ticks (one reading per device and metric per tick).
        ticks: stop after this many ticks; ``None`` means run until ``stop`` is set.
        anomaly_rate: probability that a reading is a spike, to have something to detect.
        seed: makes the stream reproducible (tests).
    """

    def __init__(
        self,
        devices: int = 5,
        interval_s: float = 1.0,
        *,
        ticks: int | None = None,
        anomaly_rate: float = 0.002,
        metrics: tuple[MetricSpec, ...] = DEFAULT_METRICS,
        seed: int | None = None,
    ) -> None:
        if devices < 1:
            raise ValueError("devices must be >= 1")
        self.device_ids = [f"device-{i:03d}" for i in range(1, devices + 1)]
        self.interval_s = interval_s
        self.ticks = ticks
        self.anomaly_rate = anomaly_rate
        self.metrics = metrics
        self._rng = random.Random(seed)
        self._state = {(d, m.name): m.mean for d in self.device_ids for m in metrics}

    def _next_value(self, device_id: str, metric: MetricSpec) -> float:
        current = self._state[(device_id, metric.name)]
        current += metric.reversion * (metric.mean - current) + self._rng.gauss(0, metric.noise)
        self._state[(device_id, metric.name)] = current
        if self._rng.random() < self.anomaly_rate:
            return current + self._rng.choice((-1, 1)) * 10 * metric.noise
        return current

    async def stream(self, stop: asyncio.Event) -> AsyncIterator[Reading]:
        tick = 0
        while not stop.is_set() and (self.ticks is None or tick < self.ticks):
            now = datetime.now(UTC)
            for device_id in self.device_ids:
                for metric in self.metrics:
                    value = round(self._next_value(device_id, metric), 3)
                    yield Reading(now, device_id, metric.name, value)
            tick += 1
            # Sleep, but wake up immediately if a shutdown is requested.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.interval_s)
