from __future__ import annotations

import re
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

from src.project_meta import get_app_meta

from ..config import DB_PATH
from ..database import get_connection
from ..migrations.versions import (
    v0001_baseline,
    v0002_v0_1_1,
    v0003_form_lifecycle,
)

MIGRATIONS_TABLE = "schema_migrations"
MIGRATION_VERSION_PATTERN = re.compile(r"^\d{4}_[a-z0-9_]+$")


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    handler: Callable[[Any], None]
    source_path: Path
    legacy_checksums: frozenset[str] = field(default_factory=frozenset)


def _metadata_only_checksum(version: str, name: str) -> str:
    return sha256(f"{version}:{name}".encode("utf-8")).hexdigest()


BASELINE_VERSION = "0001_baseline"
BASELINE_NAME = "Registra el esquema actual como baseline"

MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=BASELINE_VERSION,
        name=BASELINE_NAME,
        handler=v0001_baseline.apply,
        source_path=Path(v0001_baseline.__file__).resolve(),
        legacy_checksums=frozenset(
            {_metadata_only_checksum(BASELINE_VERSION, BASELINE_NAME)}
        ),
    ),
    Migration(
        version="0002_v0_1_1",
        name="Añade trazabilidad de workflow y formato de precio publicado",
        handler=v0002_v0_1_1.apply,
        source_path=Path(v0002_v0_1_1.__file__).resolve(),
    ),
    Migration(
        version="0003_form_lifecycle",
        name="Añade borrador, consolidación y aceptación de libros sin ISBN",
        handler=v0003_form_lifecycle.apply,
        source_path=Path(v0003_form_lifecycle.__file__).resolve(),
    ),
)


def _validate_registry() -> None:
    versions = [migration.version for migration in MIGRATIONS]
    if len(versions) != len(set(versions)):
        raise RuntimeError("El registro de migraciones contiene versiones duplicadas.")
    if versions != sorted(versions):
        raise RuntimeError("El registro de migraciones no está ordenado por versión.")
    invalid = [
        version
        for version in versions
        if not MIGRATION_VERSION_PATTERN.fullmatch(version)
    ]
    if invalid:
        raise RuntimeError(
            "El registro contiene versiones de migración no válidas: "
            + ", ".join(invalid)
        )


def _migration_checksum(migration: Migration) -> str:
    try:
        source = migration.source_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(
            f"No se pudo leer la implementación de {migration.version}: {exc}"
        ) from exc
    canonical_source = source.replace("\r\n", "\n").replace("\r", "\n")
    raw = f"{migration.version}\0{migration.name}\0{canonical_source}"
    return sha256(raw.encode("utf-8")).hexdigest()


def latest_schema_version() -> str:
    _validate_registry()
    return MIGRATIONS[-1].version


def known_schema_versions() -> tuple[str, ...]:
    _validate_registry()
    return tuple(migration.version for migration in MIGRATIONS)


def _ensure_migrations_table(con: Any) -> None:
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT now(),
            app_version TEXT,
            checksum TEXT NOT NULL
        )
        """)


def _applied_rows(con: Any) -> dict[str, dict[str, Any]]:
    rows = con.execute(f"""
        SELECT version, name, applied_at, app_version, checksum
        FROM {MIGRATIONS_TABLE}
        ORDER BY version
        """).fetchall()
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


def _validate_known_versions(applied: dict[str, dict[str, Any]]) -> None:
    known_versions = {migration.version for migration in MIGRATIONS}
    unknown_versions = sorted(set(applied) - known_versions)
    if unknown_versions:
        raise RuntimeError(
            "La base contiene migraciones que esta versión de la aplicación no "
            "conoce: " + ", ".join(unknown_versions)
        )


def _checksum_state(migration: Migration, applied: dict[str, Any]) -> str:
    if applied["name"] != migration.name:
        raise RuntimeError(
            f"La migración {migration.version} ya está aplicada con otro nombre."
        )

    expected_checksum = _migration_checksum(migration)
    stored_checksum = applied["checksum"]
    if stored_checksum == expected_checksum:
        return "current"
    if stored_checksum in migration.legacy_checksums:
        return "legacy"
    raise RuntimeError(
        f"La migración {migration.version} ya está aplicada, "
        "pero su implementación o checksum no coincide."
    )


def _build_status(applied: dict[str, dict[str, Any]]) -> dict[str, Any]:
    migrations = []
    pending_versions = []
    for migration in MIGRATIONS:
        applied_row = applied.get(migration.version)
        is_applied = applied_row is not None
        checksum_state = (
            _checksum_state(migration, applied_row) if applied_row else "pending"
        )
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
                "stored_checksum": applied_row["checksum"] if applied_row else None,
                "checksum_state": checksum_state,
            }
        )

    return {
        "migrations_table": MIGRATIONS_TABLE,
        "schema_version": latest_schema_version(),
        "known_count": len(MIGRATIONS),
        "applied_count": len(applied),
        "pending_count": len(pending_versions),
        "pending_versions": pending_versions,
        "migrations": migrations,
    }


def _run_transaction(con: Any, action: Callable[[], None]) -> None:
    con.execute("BEGIN TRANSACTION")
    try:
        action()
    except BaseException:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")


def _apply_migration(con: Any, migration: Migration, app_version: str) -> None:
    checksum = _migration_checksum(migration)

    def action() -> None:
        migration.handler(con)
        con.execute(
            f"""
            INSERT INTO {MIGRATIONS_TABLE}
                (version, name, app_version, checksum)
            VALUES (?, ?, ?, ?)
            """,
            (migration.version, migration.name, app_version, checksum),
        )

    _run_transaction(con, action)


def _upgrade_legacy_checksum(con: Any, migration: Migration) -> None:
    checksum = _migration_checksum(migration)

    def action() -> None:
        migration.handler(con)
        con.execute(
            f"""
            UPDATE {MIGRATIONS_TABLE}
            SET name = ?, checksum = ?
            WHERE version = ?
            """,
            (migration.name, checksum, migration.version),
        )

    _run_transaction(con, action)


def get_status(database_path: str | Path | None = None) -> dict[str, Any]:
    _validate_registry()
    with closing(get_connection(database_path)) as con:
        _ensure_migrations_table(con)
        applied = _applied_rows(con)
        _validate_known_versions(applied)
        return _build_status(applied)


def inspect_status(database_path: str | Path | None = None) -> dict[str, Any]:
    _validate_registry()
    target_path = Path(database_path) if database_path is not None else DB_PATH
    if not target_path.exists():
        return _build_status({})

    with closing(get_connection(target_path, read_only=True)) as con:
        table_exists = bool(
            con.execute(
                """
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_name = ?
                """,
                [MIGRATIONS_TABLE],
            ).fetchone()[0]
        )
        applied = _applied_rows(con) if table_exists else {}
        _validate_known_versions(applied)
        return _build_status(applied)


def migrate(database_path: str | Path | None = None) -> dict[str, Any]:
    _validate_registry()
    applied_now: list[str] = []
    upgraded_checksums_now: list[str] = []
    app_version = get_app_meta().version

    with closing(get_connection(database_path)) as con:
        _ensure_migrations_table(con)
        applied = _applied_rows(con)
        _validate_known_versions(applied)

        for migration in MIGRATIONS:
            applied_row = applied.get(migration.version)
            if applied_row:
                if _checksum_state(migration, applied_row) == "legacy":
                    _upgrade_legacy_checksum(con, migration)
                    upgraded_checksums_now.append(migration.version)
                continue

            _apply_migration(con, migration, app_version)
            applied_now.append(migration.version)

        status = _build_status(_applied_rows(con))
        status["applied_now"] = applied_now
        status["applied_now_count"] = len(applied_now)
        status["upgraded_checksums_now"] = upgraded_checksums_now
        return status
