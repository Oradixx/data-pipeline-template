"""A tiny, dependency-free SQL migration runner.

Files in ``db/migrations`` are named ``V<number>__<description>.sql`` and applied once,
in order. Each applied file is recorded in ``schema_migrations`` with a checksum, so
editing an already-applied migration is detected instead of silently ignored.

Some TimescaleDB statements (e.g. creating a continuous aggregate) refuse to run inside a
transaction: start such a file with ``-- migrate:no-transaction`` and its statements are
run one by one. Statements are split on ``;`` at end of line, so keep one statement per
``;`` in those files (no functions with ``$$`` bodies).
"""

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

FILENAME_RE = re.compile(r"^V(?P<version>\d+)__(?P<name>\w+)\.sql$")
NO_TRANSACTION_MARKER = "-- migrate:no-transaction"
ADVISORY_LOCK_ID = 727_001  # any constant: serialises concurrent `migrate` runs

CREATE_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT        NOT NULL,
    checksum    TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()

    @property
    def transactional(self) -> bool:
        return not self.sql.lstrip().startswith(NO_TRANSACTION_MARKER)


def discover(directory: Path) -> list[Migration]:
    """Read and sort the migration files; reject badly named files and duplicate versions."""
    if not directory.is_dir():
        raise MigrationError(f"migrations directory not found: {directory}")
    migrations: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        match = FILENAME_RE.match(path.name)
        if not match:
            raise MigrationError(f"bad migration file name: {path.name} (expected V001__name.sql)")
        version = int(match["version"])
        if version in migrations:
            raise MigrationError(f"duplicate migration version {version}: {path.name}")
        migrations[version] = Migration(version, match["name"], path.read_text(encoding="utf-8"))
    return [migrations[v] for v in sorted(migrations)]


def split_statements(sql: str) -> list[str]:
    """Split a script on semicolons that end a line; drop comment-only chunks."""
    chunks = re.split(r";\s*$", sql, flags=re.MULTILINE)
    statements = []
    for chunk in chunks:
        code = "\n".join(
            line for line in chunk.splitlines() if not line.strip().startswith("--")
        ).strip()
        if code:
            statements.append(code)
    return statements


async def apply(conn: Any, migrations: list[Migration]) -> list[Migration]:
    """Apply pending migrations and return the ones that were applied."""
    await conn.execute(CREATE_HISTORY_TABLE)
    await conn.execute("SELECT pg_advisory_lock($1)", ADVISORY_LOCK_ID)
    try:
        rows = await conn.fetch("SELECT version, checksum FROM schema_migrations")
        applied = {row["version"]: row["checksum"] for row in rows}
        newly_applied = []
        for migration in migrations:
            label = f"V{migration.version:03d}__{migration.name}"
            if migration.version in applied:
                if applied[migration.version] != migration.checksum:
                    raise MigrationError(
                        f"{label} was modified after being applied — add a new migration instead"
                    )
                continue
            log.info("applying %s", label)
            if migration.transactional:
                async with conn.transaction():
                    await conn.execute(migration.sql)
                    await _record(conn, migration)
            else:
                for statement in split_statements(migration.sql):
                    await conn.execute(statement)
                await _record(conn, migration)
            newly_applied.append(migration)
        return newly_applied
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_ID)


async def _record(conn: Any, migration: Migration) -> None:
    await conn.execute(
        "INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
        migration.version,
        migration.name,
        migration.checksum,
    )
