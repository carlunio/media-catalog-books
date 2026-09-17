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
    assert app.version == "1.0.0"


def test_schema_is_initialized_with_incremental_migrations(tmp_path, monkeypatch):
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
            ("0001_baseline", "Registra el esquema actual como baseline"),
            (
                "0002_v0_1_1",
                "Añade trazabilidad de workflow y formato de precio publicado",
            ),
            (
                "0003_form_lifecycle",
                "Añade borrador, consolidación y aceptación de libros sin ISBN",
            ),
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


def test_manual_form_lifecycle_protects_a_consolidated_book(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)
    book_id = "01A0001"

    with duckdb.connect(str(tmp_path / "books.duckdb")) as con:
        con.execute(
            """
            INSERT INTO book_items (id, block, module, seq)
            VALUES (?, 'A', '01', '0001')
            """,
            [book_id],
        )

    candidates = client.get(
        "/core-books", params={"block": "A", "module": "01", "limit": 20}
    )
    assert candidates.status_code == 200
    assert candidates.json() == [
        {
            "id": book_id,
            "titulo": None,
            "autor": None,
            "editorial": None,
            "estado_stock": None,
            "estado_carga": None,
            "precio": None,
            "block": "A",
            "module": "01",
            "form_status": "not_started",
            "form_consolidated_at": None,
            "has_core_book": False,
        }
    ]
    assert client.get(f"/core-books/{book_id}").status_code == 404

    created = client.post(f"/core-books/{book_id}/create")
    assert created.status_code == 200
    assert created.json()["book"]["form_status"] == "draft"

    saved = client.put(
        f"/core-books/{book_id}",
        json={
            "fields": {"titulo": "Ficha manual", "autor": "Autora, Ana"},
            "recompute_description": False,
        },
    )
    assert saved.status_code == 200
    assert saved.json()["book"]["titulo"] == "Ficha manual"

    consolidated = client.post(f"/core-books/{book_id}/consolidate")
    assert consolidated.status_code == 200
    assert consolidated.json()["book"]["form_status"] == "consolidated"
    assert consolidated.json()["book"]["form_consolidated_at"]

    blocked_edit = client.put(
        f"/core-books/{book_id}",
        json={"fields": {"titulo": "No debe guardarse"}},
    )
    assert blocked_edit.status_code == 409
    blocked_sync = client.post(
        f"/core-books/{book_id}/sync", params={"force_overwrite": "true"}
    )
    assert blocked_sync.status_code == 409

    blocked_workflow = client.post(
        "/workflow/run",
        json={
            "book_id": book_id,
            "block": "A",
            "module": "01",
            "start_stage": "ocr",
            "overwrite": True,
        },
    )
    assert blocked_workflow.status_code == 200
    assert blocked_workflow.json()["items"][0]["status"] == "skipped"

    with duckdb.connect(str(tmp_path / "books.duckdb")) as con:
        state = con.execute(
            """
            SELECT form_status, workflow_status, workflow_current_node,
                   pipeline_stage, workflow_needs_review
            FROM book_items
            WHERE id = ?
            """,
            [book_id],
        ).fetchone()
        title = con.execute(
            "SELECT titulo FROM books WHERE id = ?", [book_id]
        ).fetchone()
    assert state == ("consolidated", "done", "form_consolidated", "done", False)
    assert title == ("Ficha manual",)

    stats = client.get("/stats", params={"block": "A", "module": "01"})
    assert stats.status_code == 200
    assert stats.json() == {
        "total": 1,
        "needs_ocr": 0,
        "needs_metadata": 0,
        "needs_catalog": 0,
        "needs_cover": 0,
        "needs_workflow_review": 0,
        "form_consolidated": 1,
    }

    reopened = client.post(f"/core-books/{book_id}/reopen")
    assert reopened.status_code == 200
    assert reopened.json()["book"]["form_status"] == "draft"
    edited = client.put(
        f"/core-books/{book_id}",
        json={"fields": {"titulo": "Ficha corregida"}},
    )
    assert edited.status_code == 200
    assert edited.json()["book"]["titulo"] == "Ficha corregida"


def test_approved_book_without_isbn_can_continue_to_catalog(tmp_path, monkeypatch):
    app = _load_app(tmp_path, monkeypatch)
    client = TestClient(app)
    book_id = "01A0002"

    with duckdb.connect(str(tmp_path / "books.duckdb")) as con:
        con.execute(
            """
            INSERT INTO book_items (
                id, block, module, seq, ocr_status,
                workflow_status, workflow_current_node,
                workflow_needs_review, workflow_review_reason, pipeline_stage
            )
            VALUES (
                ?, 'A', '01', '0002', 'processed',
                'review', 'ocr_isbn_validation',
                TRUE, 'ocr_isbn_validation: no valid ISBN', 'review'
            )
            """,
            [book_id],
        )
        con.execute(
            """
            INSERT INTO book_ocr_data (book_id, extracted_text)
            VALUES (?, 'Título y créditos suficientes, pero sin ISBN')
            """,
            [book_id],
        )

    approved = client.post(
        f"/workflow/review/{book_id}",
        json={"action": "approve"},
    )
    assert approved.status_code == 200
    result = approved.json()["result"]
    assert result["status"] == "approved"
    assert result["target_stage"] == "metadata"

    after_approval = client.get(f"/books/{book_id}").json()
    assert after_approval["isbn"] is None
    assert after_approval["isbn_missing_accepted_at"]
    assert after_approval["workflow_needs_review"] is False

    metadata = client.post(
        "/metadata/fetch",
        json={
            "book_id": book_id,
            "block": "A",
            "module": "01",
            "overwrite": False,
        },
    )
    assert metadata.status_code == 200, metadata.text
    assert metadata.json()["items"][0]["status"] == "partial"

    after_metadata = client.get(f"/books/{book_id}").json()
    assert after_metadata["metadata_status"] == "skipped"
    assert after_metadata["pipeline_stage"] == "catalog"
    assert after_metadata["workflow_needs_review"] is False

    graph = importlib.import_module("src.backend.workflow.graph")
    catalog = importlib.import_module("src.backend.services.catalog")
    monkeypatch.setattr(
        catalog,
        "_call_catalog_llm",
        lambda **_kwargs: '{"titulo": "Libro sin ISBN", "autor": ["Autora, Ana"]}',
    )
    assert graph._should_route_to_ocr_review(after_metadata) is False

    catalog_result = catalog.run_one(book_id, overwrite=False)
    assert catalog_result["status"] == "built"
    core_book = client.get(f"/core-books/{book_id}").json()
    assert core_book["titulo"] == "Libro sin ISBN"
    assert core_book["form_status"] == "draft"


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
    assert status.json()["schema_version"] == "0003_form_lifecycle"

    published = client.post(
        "/snapshots/publish",
        json={"notes": "test snapshot", "cleanup": False},
    )
    assert published.status_code == 200
    snapshot = published.json()["snapshot"]
    snapshot_id = snapshot["snapshot_id"]
    assert snapshot["valid"] is True
    assert snapshot["importable"] is True
    assert snapshot["compatibility"] == "current"
    assert snapshot["schema_version"] == "0003_form_lifecycle"

    listed = client.get("/snapshots")
    assert listed.status_code == 200
    assert [item["snapshot_id"] for item in listed.json()["snapshots"]] == [snapshot_id]

    blocked_import = client.post(
        "/snapshots/import",
        json={"snapshot_id": snapshot_id, "confirm": False},
    )
    assert blocked_import.status_code == 400

    imported = client.post(
        "/snapshots/import",
        json={"snapshot_id": snapshot_id, "confirm": True},
    )
    assert imported.status_code == 200
    assert imported.json()["migration"]["schema_version"] == "0003_form_lifecycle"
    assert imported.json()["restart_required"] is True
