"""Settings read from environment variables (12-factor style) — see .env.example."""

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    batch_size: int = 500
    batch_max_wait_s: float = 2.0
    devices: int = 5
    interval_s: float = 1.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        try:
            database_url = os.environ["DATABASE_URL"]
        except KeyError:
            raise RuntimeError("DATABASE_URL is not set — copy .env.example to .env") from None
        return cls(
            database_url=database_url,
            batch_size=int(os.getenv("BATCH_SIZE", "500")),
            batch_max_wait_s=float(os.getenv("BATCH_MAX_WAIT_S", "2.0")),
            devices=int(os.getenv("SOURCE_DEVICES", "5")),
            interval_s=float(os.getenv("SOURCE_INTERVAL_S", "1.0")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )
