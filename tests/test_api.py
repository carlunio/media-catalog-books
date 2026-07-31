import importlib
import sys
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

EXPORT_COLUMNS = [
    "listingid",
    "title",
    "author",
    "publishername",
    "isbn",
    "language",
    "producttype",
    "bindingtext",
    "bookcondition",
    "keywords",
    "imgurl",
    "price",
    "quantity",
    "description",
]


def _configure_tmp_env(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))
    monkeypatch.setenv("DB_PATH", str(tmp_path / "books.duckdb"))
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
    monkeypatch.setenv("GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("OPENLIBRARY_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("REQUEST_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("WORKFLOW_MAX_ATTEMPTS", "2")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ISBNDB_API_KEY", raising=False)


def _clear_src_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)
        if module_name == "backend" or module_name.startswith("backend."):
            sys.modules.pop(module_name, None)


def _load_app(tmp_path: Path, monkeypatch):
    _configure_tmp_env(tmp_path, monkeypatch)
    _clear_src_modules()
    main = importlib.import_module("src.backend.main")
    return main.app


def test_backend_imports_without_external_api_keys(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert app.version == "0.1.0"


def test_schema_is_initialized_with_migration_baseline(tmp_path, monkeypatch):
    _load_app(tmp_path, monkeypatch)

    with duckdb.connect(str(tmp_path / "books.duckdb")) as con:
        tables = {str(row[0]) for row in con.execute("PRAGMA show_tables").fetchall()}
        assert "book_items" in tables
        assert "book_image_files" in tables
        assert "book_ocr_data" in tables
        assert "book_bibliographic_sources" in tables
        assert "books" in tables
        assert "book_field_allowed_values" in tables
        assert "libros_carga_abebooks" in tables
        assert "schema_migrations" in tables

        migrations = con.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert migrations == [
            ("0001_baseline", "Registra el esquema actual como baseline")
        ]

        relation = con.execute(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND table_name = ?
            """,
            ("libros_carga_abebooks",),
        ).fetchone()
        assert relation is not None
        assert str(relation[0]).upper() == "VIEW"


def test_export_view_and_preview_contract(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    with duckdb.connect(str(tmp_path / "books.duckdb")) as con:
        con.execute(
            """
            INSERT INTO books (
                id, estado_carga, titulo, autor, editorial, isbn, idioma,
                tipo_articulo, encuadernacion, estado_conservacion,
                palabras_clave, url_imagenes, precio, cantidad, descripcion
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "01A0001",
                "Para subir",
                "Libro de prueba",
                "Autor, Ana",
                "Editorial",
                "9788490000000",
                "espanol",
                "Libros",
                "Tapa blanda",
                "Bien",
                "prueba",
                "01A0001.jpg",
                12.5,
                1,
                "Descripcion de prueba",
            ),
        )

        cur = con.execute("SELECT * FROM libros_carga_abebooks")
        assert [desc[0] for desc in cur.description] == EXPORT_COLUMNS
        row = dict(zip(EXPORT_COLUMNS, cur.fetchone()))
        assert row["listingid"] == "01A0001"
        assert row["price"] == "12.50 €"

    response = client.get("/export/books/preview", params={"limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload["columns"] == EXPORT_COLUMNS
    assert payload["count"] == 1
    assert payload["ids"] == ["01A0001"]
    assert payload["validation"]["invalid_count"] == 0


def test_snapshot_endpoints_publish_and_list(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)

    status = client.get("/snapshots/status")
    assert status.status_code == 200
    assert status.json()["local_db_exists"] is True

    published = client.post(
        "/snapshots/publish",
        json={"notes": "test snapshot", "cleanup": False},
    )
    assert published.status_code == 200
    snapshot = published.json()["snapshot"]
    snapshot_id = snapshot["snapshot_id"]
    assert snapshot["valid"] is True

    listed = client.get("/snapshots")
    assert listed.status_code == 200
    assert [item["snapshot_id"] for item in listed.json()["snapshots"]] == [snapshot_id]

    blocked_import = client.post(
        "/snapshots/import",
        json={"snapshot_id": snapshot_id, "confirm": False},
    )
    assert blocked_import.status_code == 400
