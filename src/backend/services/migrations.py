from __future__ import annotations

from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from src.project_meta import get_app_meta

from ..database import get_connection

MIGRATIONS_TABLE = "schema_migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    handler: Callable[[Any], None]


def _baseline(con: Any) -> None:
    from . import books

    books.ensure_schema(con)


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version="0001_baseline",
        name="Registra el esquema actual como baseline",
        handler=_baseline,
    ),
)


def _migration_checksum(migration: Migration) -> str:
    raw = f"{migration.version}:{migration.name}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _ensure_migrations_table(con: Any) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT now(),
            app_version TEXT,
            checksum TEXT NOT NULL
        )
        """
    )


def _applied_rows(con: Any) -> dict[str, dict[str, Any]]:
    rows = con.execute(
        f"""
        SELECT version, name, applied_at, app_version, checksum
        FROM {MIGRATIONS_TABLE}
        ORDER BY version
        """
    ).fetchall()
    return {
        str(row[0]): {
            "version": str(row[0]),
            "name": str(row[1]),
            "applied_at": str(row[2]) if row[2] is not None else None,
            "app_version": str(row[3]) if row[3] is not None else None,
            "checksum": str(row[4]),
        }
        for row in rows
    }


def _validate_applied_migration(migration: Migration, applied: dict[str, Any]) -> None:
    expected_checksum = _migration_checksum(migration)
    if applied["checksum"] != expected_checksum:
        raise RuntimeError(
            f"La migracion {migration.version} ya esta aplicada, "
            "pero su checksum no coincide."
        )


def _build_status(applied: dict[str, dict[str, Any]]) -> dict[str, Any]:
    migrations = []
    pending_versions = []
    for migration in MIGRATIONS:
        applied_row = applied.get(migration.version)
        is_applied = applied_row is not None
        if not is_applied:
            pending_versions.append(migration.version)
        migrations.append(
            {
                "version": migration.version,
                "name": migration.name,
                "applied": is_applied,
                "applied_at": applied_row["applied_at"] if applied_row else None,
                "app_version": applied_row["app_version"] if applied_row else None,
                "checksum": _migration_checksum(migration),
            }
        )

    return {
        "migrations_table": MIGRATIONS_TABLE,
        "known_count": len(MIGRATIONS),
        "applied_count": len(applied),
        "pending_count": len(pending_versions),
        "pending_versions": pending_versions,
        "migrations": migrations,
    }


def get_status() -> dict[str, Any]:
    with closing(get_connection()) as con:
        _ensure_migrations_table(con)
        return _build_status(_applied_rows(con))


def migrate() -> dict[str, Any]:
    applied_now: list[str] = []
    app_version = get_app_meta().version

    with closing(get_connection()) as con:
        _ensure_migrations_table(con)
        applied = _applied_rows(con)
        for migration in MIGRATIONS:
            applied_row = applied.get(migration.version)
            if applied_row:
                _validate_applied_migration(migration, applied_row)
                continue

            migration.handler(con)
            checksum = _migration_checksum(migration)
            con.execute(
                f"""
                INSERT INTO {MIGRATIONS_TABLE}
                    (version, name, app_version, checksum)
                VALUES (?, ?, ?, ?)
                """,
                (migration.version, migration.name, app_version, checksum),
            )
            applied_now.append(migration.version)

        status = _build_status(_applied_rows(con))
        status["applied_now"] = applied_now
        status["applied_now_count"] = len(applied_now)
        return status
