from pathlib import Path

import pytest

from pipeline.migrate import MigrationError, discover, split_statements

REPO_MIGRATIONS = Path(__file__).parents[2] / "db" / "migrations"


def test_repository_migrations_are_valid_and_ordered() -> None:
    migrations = discover(REPO_MIGRATIONS)
    versions = [m.version for m in migrations]
    assert versions == sorted(versions) == list(range(1, len(versions) + 1))


def test_continuous_aggregate_migration_runs_outside_a_transaction() -> None:
    by_name = {m.name: m for m in discover(REPO_MIGRATIONS)}
    assert not by_name["hourly_continuous_aggregate"].transactional
    assert by_name["create_sensor_readings"].transactional


def test_numeric_ordering(tmp_path: Path) -> None:
    for name in ("V10__ten.sql", "V2__two.sql", "V1__one.sql"):
        (tmp_path / name).write_text("SELECT 1;")
    assert [m.version for m in discover(tmp_path)] == [1, 2, 10]


@pytest.mark.parametrize("names", [["001_bad_name.sql"], ["V1__a.sql", "V01__b.sql"]])
def test_bad_names_and_duplicates_are_rejected(tmp_path: Path, names: list[str]) -> None:
    for name in names:
        (tmp_path / name).write_text("SELECT 1;")
    with pytest.raises(MigrationError):
        discover(tmp_path)


def test_checksum_changes_with_content(tmp_path: Path) -> None:
    path = tmp_path / "V1__a.sql"
    path.write_text("SELECT 1;")
    before = discover(tmp_path)[0].checksum
    path.write_text("SELECT 2;")
    assert discover(tmp_path)[0].checksum != before


def test_split_statements_drops_comments() -> None:
    sql = "-- header\nCREATE TABLE t (a int);\n\n-- note\nSELECT f(\n  1\n);\n-- trailing\n"
    assert split_statements(sql) == ["CREATE TABLE t (a int)", "SELECT f(\n  1\n)"]
