import gzip
import importlib
import shutil
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "books-v0.1.1.duckdb.gz"


def _clear_src_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)
        if module_name == "backend" or module_name.startswith("backend."):
            sys.modules.pop(module_name, None)


def _load_services(tmp_path: Path, monkeypatch):
    database_path = tmp_path / "data" / "books.duckdb"
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(database_path))
    monkeypatch.setenv("COVERS_DIR", str(tmp_path / "input"))
    monkeypatch.setenv("COVERS_OUTPUT_DIR", str(tmp_path / "output" / "covers"))
    monkeypatch.setenv("EXPORTS_DIR", str(tmp_path / "output" / "exports"))
    monkeypatch.setenv("OCR_OUTPUT_DIR", str(tmp_path / "ocr_output"))
    monkeypatch.setenv("BBDD_DIR", str(tmp_path / "bbdd"))
    monkeypatch.setenv("SYNC_STATE_PATH", str(tmp_path / "sync_state.json"))
    monkeypatch.setenv("SYNC_ACTOR", "test-user")
    monkeypatch.setenv("SYNC_DEVICE", "test-device")
    monkeypatch.setenv("SYNC_RETENTION_DAYS", "14")
    monkeypatch.setenv("SYNC_KEEP_MIN", "10")
    _clear_src_modules()
    migrations = importlib.import_module("src.backend.services.migrations")
    snapshots = importlib.import_module("src.backend.services.snapshots")
    migrations.migrate()
    return migrations, snapshots, database_path


def _add_local_book(database_path: Path, *, title: str = "Base local") -> None:
    with duckdb.connect(str(database_path)) as con:
        con.execute(
            """
            INSERT INTO books (id, titulo, autor, precio, descripcion)
            VALUES ('LOCAL001', ?, 'Local, Usuario', 5.5, 'Dato local')
            ON CONFLICT (id) DO UPDATE SET titulo = excluded.titulo
            """,
            (title,),
        )


def _install_snapshot(
    snapshots,
    source_path: Path,
    *,
    snapshot_id: str,
    schema_version: str,
) -> dict:
    snapshots_dir = snapshots._snapshots_dir()
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    target_path = snapshots_dir / f"{snapshot_id}.duckdb"
    shutil.copy2(source_path, target_path)
    manifest = {
        "snapshot_id": snapshot_id,
        "created_at": "2026-09-12T12:00:00+02:00",
        "app_version": "0.1.1",
        "schema_version": schema_version,
        "source_actor": "remote-user",
        "source_device": "remote-device",
        "db_filename": target_path.name,
        "db_size_bytes": target_path.stat().st_size,
        "sha256": snapshots._sha256_file(target_path),
        "protected": False,
        "notes": "Test snapshot",
    }
    snapshots._write_json_atomic(snapshots_dir / f"{snapshot_id}.json", manifest)
    return manifest


def _decompress_v0_1_1(target_path: Path) -> None:
    with gzip.open(FIXTURE_PATH, "rb") as source, target_path.open("wb") as target:
        shutil.copyfileobj(source, target)


def test_publish_records_current_schema_and_is_importable(tmp_path, monkeypatch):
    migrations, snapshots, _database_path = _load_services(tmp_path, monkeypatch)

    result = snapshots.publish_snapshot(notes="Current schema", cleanup=False)

    snapshot = result["snapshot"]
    assert snapshot["schema_version"] == migrations.latest_schema_version()
    assert snapshot["compatibility"] == "current"
    assert snapshot["migration_required"] is False
    assert snapshot["valid"] is True
    assert snapshot["importable"] is True


def test_import_migrates_v0_1_1_candidate_and_preserves_local_backup(
    tmp_path, monkeypatch
):
    migrations, snapshots, database_path = _load_services(tmp_path, monkeypatch)
    _add_local_book(database_path)
    fixture_database = tmp_path / "v0.1.1.duckdb"
    _decompress_v0_1_1(fixture_database)
    manifest = _install_snapshot(
        snapshots,
        fixture_database,
        snapshot_id="legacy-v0-1-1",
        schema_version="1",
    )

    listed = snapshots.list_snapshots()
    assert listed[0]["compatibility"] == "upgrade_required"
    assert listed[0]["importable"] is True

    result = snapshots.import_snapshot(
        snapshot_id=manifest["snapshot_id"], confirm=True
    )

    assert result["migration"]["source_schema_version"] == "1"
    assert result["migration"]["schema_version"] == migrations.latest_schema_version()
    assert result["migration"]["applied_now"] == [
        "0001_baseline",
        "0002_v0_1_1",
        "0003_form_lifecycle",
    ]
    assert result["restart_required"] is True
    assert (
        snapshots._sha256_file(Path(result["snapshot"]["path"])) == manifest["sha256"]
    )

    with duckdb.connect(str(database_path)) as con:
        imported = con.execute(
            "SELECT titulo, autor, precio FROM books WHERE id = '03B0001'"
        ).fetchone()
        local_count = con.execute(
            "SELECT COUNT(*) FROM books WHERE id = 'LOCAL001'"
        ).fetchone()[0]
        versions = con.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert imported == (
        "Edición manual v0.1.1",
        "Autora, Ana",
        Decimal("12.34"),
    )
    assert local_count == 0
    assert versions == [
        ("0001_baseline",),
        ("0002_v0_1_1",),
        ("0003_form_lifecycle",),
    ]

    backup_path = Path(result["backup_path"])
    with duckdb.connect(str(backup_path), read_only=True) as con:
        assert con.execute(
            "SELECT titulo FROM books WHERE id = 'LOCAL001'"
        ).fetchone() == ("Base local",)


def test_future_manifest_is_not_importable_and_keeps_local_database(
    tmp_path, monkeypatch
):
    _migrations, snapshots, database_path = _load_services(tmp_path, monkeypatch)
    _add_local_book(database_path)
    _install_snapshot(
        snapshots,
        database_path,
        snapshot_id="future",
        schema_version="9999_future",
    )

    listed = snapshots.list_snapshots()
    assert listed[0]["valid"] is True
    assert listed[0]["compatible"] is False
    assert listed[0]["importable"] is False
    with pytest.raises(snapshots.SnapshotError, match="no es compatible"):
        snapshots.import_snapshot(snapshot_id="future", confirm=True)

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            "SELECT titulo FROM books WHERE id = 'LOCAL001'"
        ).fetchone() == ("Base local",)
    assert not (database_path.parent / "backups" / "local").exists()


def test_manifest_cannot_reference_database_outside_snapshot_directory(
    tmp_path, monkeypatch
):
    migrations, snapshots, _database_path = _load_services(tmp_path, monkeypatch)
    outside_path = snapshots._snapshots_dir().parent / "outside.duckdb"
    outside_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(_database_path, outside_path)
    manifest = {
        "snapshot_id": "path-traversal",
        "created_at": "2026-09-12T12:00:00+02:00",
        "schema_version": migrations.latest_schema_version(),
        "db_filename": "../outside.duckdb",
        "sha256": snapshots._sha256_file(outside_path),
    }
    snapshots._snapshots_dir().mkdir(parents=True, exist_ok=True)
    snapshots._write_json_atomic(
        snapshots._snapshots_dir() / "path-traversal.json", manifest
    )

    listed = snapshots.list_snapshots()

    assert listed[0]["valid"] is False
    assert listed[0]["importable"] is False
    assert "nombre de fichero simple" in listed[0]["error"]
    snapshots.cleanup_snapshots()
    assert outside_path.exists()


def test_foreign_duckdb_is_rejected_before_backup_or_replacement(tmp_path, monkeypatch):
    migrations, snapshots, database_path = _load_services(tmp_path, monkeypatch)
    _add_local_book(database_path)
    foreign_path = tmp_path / "movies.duckdb"
    with duckdb.connect(str(foreign_path)) as con:
        con.execute("CREATE TABLE movies (id VARCHAR PRIMARY KEY, title VARCHAR)")
    _install_snapshot(
        snapshots,
        foreign_path,
        snapshot_id="foreign",
        schema_version=migrations.latest_schema_version(),
    )

    with pytest.raises(snapshots.SnapshotError, match="no pertenece"):
        snapshots.import_snapshot(snapshot_id="foreign", confirm=True)

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            "SELECT titulo FROM books WHERE id = 'LOCAL001'"
        ).fetchone() == ("Base local",)
    assert not (database_path.parent / "backups" / "local").exists()


def test_database_with_hidden_future_migration_is_rejected(tmp_path, monkeypatch):
    migrations, snapshots, database_path = _load_services(tmp_path, monkeypatch)
    _add_local_book(database_path)
    manifest = _install_snapshot(
        snapshots,
        database_path,
        snapshot_id="hidden-future",
        schema_version=migrations.latest_schema_version(),
    )
    snapshot_path = snapshots._snapshots_dir() / manifest["db_filename"]
    with duckdb.connect(str(snapshot_path)) as con:
        con.execute("""
            INSERT INTO schema_migrations (version, name, app_version, checksum)
            VALUES ('9999_future', 'Future migration', '99.0.0', 'checksum')
            """)
    manifest["sha256"] = snapshots._sha256_file(snapshot_path)
    snapshots._write_json_atomic(
        snapshots._snapshots_dir() / "hidden-future.json", manifest
    )

    with pytest.raises(snapshots.SnapshotError, match="9999_future"):
        snapshots.import_snapshot(snapshot_id="hidden-future", confirm=True)

    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            "SELECT titulo FROM books WHERE id = 'LOCAL001'"
        ).fetchone() == ("Base local",)
    assert not (database_path.parent / "backups" / "local").exists()


def test_import_restores_local_database_when_state_write_fails(tmp_path, monkeypatch):
    migrations, snapshots, database_path = _load_services(tmp_path, monkeypatch)
    _add_local_book(database_path, title="Snapshot publicado")
    published = snapshots.publish_snapshot(notes="Rollback test", cleanup=False)
    _add_local_book(database_path, title="Estado local posterior")

    def fail_state_write(*_args, **_kwargs):
        raise OSError("state unavailable")

    monkeypatch.setattr(snapshots, "_update_import_state", fail_state_write)

    with pytest.raises(snapshots.SnapshotError, match="se ha restaurado"):
        snapshots.import_snapshot(
            snapshot_id=published["snapshot"]["snapshot_id"], confirm=True
        )

    with duckdb.connect(str(database_path)) as con:
        title = con.execute(
            "SELECT titulo FROM books WHERE id = 'LOCAL001'"
        ).fetchone()[0]
        versions = con.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert title == "Estado local posterior"
    assert versions == [(version,) for version in migrations.known_schema_versions()]
    assert not list(database_path.parent.glob(".*.importing_*.duckdb"))
    assert not list(database_path.parent.glob(".*.restoring_*.duckdb"))
