import gzip
import importlib
import shutil
import sys
from pathlib import Path

import duckdb
import pytest

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "books-v0.1.1.duckdb.gz"


def _configure_tmp_env(tmp_path: Path, monkeypatch) -> Path:
    database_path = tmp_path / "books.duckdb"
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
    return database_path


def _clear_src_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)
        if module_name == "backend" or module_name.startswith("backend."):
            sys.modules.pop(module_name, None)


def _load_migrations(tmp_path: Path, monkeypatch):
    database_path = _configure_tmp_env(tmp_path, monkeypatch)
    _clear_src_modules()
    migrations = importlib.import_module("src.backend.services.migrations")
    return migrations, database_path


def test_migrations_are_incremental_and_idempotent(tmp_path, monkeypatch):
    migrations, _database_path = _load_migrations(tmp_path, monkeypatch)

    first = migrations.migrate()
    second = migrations.migrate()

    assert first["applied_now"] == [
        "0001_baseline",
        "0002_v0_1_1",
        "0003_form_lifecycle",
    ]
    assert first["schema_version"] == "0003_form_lifecycle"
    assert second["applied_now"] == []
    assert second["pending_count"] == 0
    assert all(item["checksum_state"] == "current" for item in second["migrations"])


def test_read_only_inspection_does_not_create_a_missing_database(tmp_path, monkeypatch):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)

    status = migrations.inspect_status(database_path)

    assert status["pending_versions"] == [
        "0001_baseline",
        "0002_v0_1_1",
        "0003_form_lifecycle",
    ]
    assert not database_path.exists()


def test_migrations_can_prepare_a_candidate_without_touching_the_active_database(
    tmp_path, monkeypatch
):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    candidate_path = tmp_path / "candidate.duckdb"

    status = migrations.migrate(candidate_path)

    assert status["schema_version"] == migrations.latest_schema_version()
    assert candidate_path.exists()
    assert not database_path.exists()
    with duckdb.connect(str(candidate_path)) as con:
        applied = con.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert applied == [
        ("0001_baseline",),
        ("0002_v0_1_1",),
        ("0003_form_lifecycle",),
    ]


def test_migration_checksum_covers_implementation_source(tmp_path, monkeypatch):
    migrations, _database_path = _load_migrations(tmp_path, monkeypatch)
    source_path = tmp_path / "v9999_test.py"
    source_path.write_text("def apply(con):\n    return None\n", encoding="utf-8")
    migration = migrations.Migration(
        version="9999_test",
        name="Test migration",
        handler=lambda _con: None,
        source_path=source_path,
    )

    checksum_before = migrations._migration_checksum(migration)
    source_path.write_text(
        "def apply(con):\n    con.execute('SELECT 1')\n", encoding="utf-8"
    )
    checksum_after = migrations._migration_checksum(migration)

    assert checksum_before != checksum_after


def test_migrations_reject_an_altered_applied_checksum(tmp_path, monkeypatch):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    migrations.migrate()

    with duckdb.connect(str(database_path)) as con:
        con.execute(
            "UPDATE schema_migrations SET checksum = 'altered' "
            "WHERE version = '0002_v0_1_1'"
        )

    with pytest.raises(RuntimeError, match="implementación o checksum"):
        migrations.migrate()


def test_migrations_reject_a_database_newer_than_the_app(tmp_path, monkeypatch):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    migrations.migrate()

    with duckdb.connect(str(database_path)) as con:
        con.execute("""
            INSERT INTO schema_migrations (version, name, app_version, checksum)
            VALUES ('9999_future', 'Future migration', '99.0.0', 'checksum')
            """)

    with pytest.raises(RuntimeError, match="no conoce: 9999_future"):
        migrations.migrate()


def test_failed_migration_rolls_back_schema_and_history(tmp_path, monkeypatch):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    source_path = tmp_path / "v0001_failure.py"
    source_path.write_text(
        "def apply(con):\n    raise RuntimeError\n", encoding="utf-8"
    )

    def failing_handler(con):
        con.execute("CREATE TABLE partial_change (id INTEGER)")
        raise RuntimeError("migration failed")

    monkeypatch.setattr(
        migrations,
        "MIGRATIONS",
        (
            migrations.Migration(
                version="0001_failure",
                name="Failing migration",
                handler=failing_handler,
                source_path=source_path,
            ),
        ),
    )

    with pytest.raises(RuntimeError, match="migration failed"):
        migrations.migrate()

    with duckdb.connect(str(database_path)) as con:
        partial_exists = con.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'partial_change'
            """).fetchone()[0]
        history_count = con.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0]
    assert partial_exists == 0
    assert history_count == 0


def test_legacy_baseline_checksum_is_upgraded_safely(tmp_path, monkeypatch):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    legacy_checksum = migrations._metadata_only_checksum(
        migrations.BASELINE_VERSION, migrations.BASELINE_NAME
    )

    with duckdb.connect(str(database_path)) as con:
        migrations._ensure_migrations_table(con)
        con.execute(
            """
            INSERT INTO schema_migrations (version, name, app_version, checksum)
            VALUES (?, ?, '0.1.1', ?)
            """,
            (
                migrations.BASELINE_VERSION,
                migrations.BASELINE_NAME,
                legacy_checksum,
            ),
        )

    status = migrations.migrate()

    assert status["upgraded_checksums_now"] == ["0001_baseline"]
    assert status["applied_now"] == ["0002_v0_1_1", "0003_form_lifecycle"]
    assert all(item["checksum_state"] == "current" for item in status["migrations"])
    with duckdb.connect(str(database_path)) as con:
        assert con.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = 'books'
            """).fetchone()[0] == 1


def test_migrates_real_v0_1_1_database_without_changing_user_data(
    tmp_path, monkeypatch
):
    migrations, database_path = _load_migrations(tmp_path, monkeypatch)
    with gzip.open(FIXTURE_PATH, "rb") as source, database_path.open("wb") as target:
        shutil.copyfileobj(source, target)

    with duckdb.connect(str(database_path)) as con:
        before_book = con.execute("""
            SELECT titulo, autor, precio, descripcion
            FROM books
            WHERE id = '03B0001'
            """).fetchone()
        before_tables = con.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """).fetchall()
        before_columns = con.execute("""
            SELECT table_schema, table_name, column_name, data_type,
                   is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
            """).fetchall()

    status = migrations.migrate()

    with duckdb.connect(str(database_path)) as con:
        after_book = con.execute("""
            SELECT titulo, autor, precio, descripcion
            FROM books
            WHERE id = '03B0001'
            """).fetchone()
        after_tables = con.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
              AND table_name <> 'schema_migrations'
            ORDER BY table_schema, table_name
            """).fetchall()
        after_columns = con.execute("""
            SELECT table_schema, table_name, column_name, data_type,
                   is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
              AND table_name <> 'schema_migrations'
            ORDER BY table_schema, table_name, ordinal_position
            """).fetchall()
        exported_price = con.execute("""
            SELECT price
            FROM libros_carga_abebooks
            WHERE listingid = '03B0001'
            """).fetchone()[0]
        related_counts = con.execute("""
            SELECT
                (SELECT COUNT(*) FROM book_items WHERE id = '03B0001'),
                (SELECT COUNT(*) FROM book_image_files WHERE book_id = '03B0001'),
                (SELECT COUNT(*) FROM book_ocr_data WHERE book_id = '03B0001'),
                (SELECT COUNT(*) FROM book_bibliographic_sources
                 WHERE book_id = '03B0001')
            """).fetchone()

    assert status["applied_now"] == [
        "0001_baseline",
        "0002_v0_1_1",
        "0003_form_lifecycle",
    ]
    assert before_book == after_book
    assert before_tables == after_tables
    lifecycle_columns = {
        "form_status",
        "form_consolidated_at",
        "isbn_missing_accepted_at",
    }
    preserved_after_columns = [
        row
        for row in after_columns
        if not (row[1] == "book_items" and row[2] in lifecycle_columns)
    ]
    assert before_columns == preserved_after_columns
    assert str(exported_price) == "12.34 €"
    assert related_counts == (1, 1, 1, 1)
    with duckdb.connect(str(database_path)) as con:
        assert con.execute(
            "SELECT form_status FROM book_items WHERE id = '03B0001'"
        ).fetchone() == ("draft",)
