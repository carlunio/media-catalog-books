from types import SimpleNamespace

from src import project_meta
from src.project_meta import get_app_meta


def test_project_meta_reads_pyproject_version():
    meta = get_app_meta()

    assert meta.project_name == "media-catalog-books"
    assert meta.version == "0.1.1"
    assert meta.changelog_path.name == "CHANGELOG.md"


def test_project_meta_falls_back_to_installed_distribution(tmp_path, monkeypatch):
    installed = SimpleNamespace(
        metadata={"Name": "media-catalog-books"},
        version="9.8.7",
    )
    monkeypatch.setattr(project_meta, "PYPROJECT_PATH", tmp_path / "missing.toml")
    monkeypatch.setattr(project_meta, "distribution", lambda _name: installed)
    get_app_meta.cache_clear()

    try:
        meta = get_app_meta()
    finally:
        get_app_meta.cache_clear()

    assert meta.project_name == "media-catalog-books"
    assert meta.version == "9.8.7"
