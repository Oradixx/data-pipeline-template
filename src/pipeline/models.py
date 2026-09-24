"""The record that flows through the pipeline — one row of ``sensor_readings``."""

from datetime import datetime
from typing import NamedTuple


class Reading(NamedTuple):
    """A NamedTuple is a plain tuple, which is exactly what asyncpg's COPY expects."""

    time: datetime
    device_id: str
    metric: str
    value: float


COLUMNS = list(Reading._fields)
