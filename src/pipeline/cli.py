"""``pipeline migrate`` and ``pipeline ingest`` commands."""

import argparse
import asyncio
import logging
import os
from pathlib import Path

import asyncpg

from pipeline.config import Settings
from pipeline.ingest import ingest
from pipeline.migrate import apply, discover


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    migrate = sub.add_parser("migrate", help="apply pending SQL migrations")
    migrate.add_argument(
        "--dir",
        type=Path,
        default=Path(os.getenv("MIGRATIONS_DIR", "db/migrations")),
        help="migrations folder (default: $MIGRATIONS_DIR or db/migrations)",
    )

    run = sub.add_parser("ingest", help="stream data into the database")
    run.add_argument("--ticks", type=int, default=None, help="stop after N ticks (default: run)")
    return parser


async def _migrate(settings: Settings, directory: Path) -> None:
    migrations = discover(directory)
    conn = await asyncpg.connect(settings.database_url)
    try:
        applied = await apply(conn, migrations)
    finally:
        await conn.close()
    logging.getLogger("pipeline").info(
        "%d migration(s) applied, %d already up to date",
        len(applied),
        len(migrations) - len(applied),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    if args.command == "migrate":
        asyncio.run(_migrate(settings, args.dir))
    else:
        asyncio.run(ingest(settings, ticks=args.ticks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
