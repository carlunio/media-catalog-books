import importlib
import sys
from pathlib import Path


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


def _clear_src_modules() -> None:
    for module_name in list(sys.modules):
        if module_name == "src" or module_name.startswith("src."):
            sys.modules.pop(module_name, None)
        if module_name == "backend" or module_name.startswith("backend."):
            sys.modules.pop(module_name, None)


def test_migrations_are_idempotent(tmp_path, monkeypatch):
    _configure_tmp_env(tmp_path, monkeypatch)
    _clear_src_modules()

    migrations = importlib.import_module("src.backend.services.migrations")

    first = migrations.migrate()
    second = migrations.migrate()

    assert first["applied_now"] == ["0001_baseline"]
    assert second["applied_now"] == []
    assert second["pending_count"] == 0
