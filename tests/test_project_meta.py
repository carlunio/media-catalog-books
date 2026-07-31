from src.project_meta import get_app_meta


def test_project_meta_reads_pyproject_version():
    meta = get_app_meta()

    assert meta.project_name == "media-catalog-books"
    assert meta.version == "0.1.0"
    assert meta.changelog_path.name == "CHANGELOG.md"
