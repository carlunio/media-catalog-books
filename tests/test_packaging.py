import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from setuptools import find_namespace_packages

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"


def _dependency_names(requirements: list[str]) -> set[str]:
    return {canonicalize_name(Requirement(item).name) for item in requirements}


def _pyproject_document() -> dict:
    return tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))


def test_pyproject_is_the_only_dependency_manifest():
    document = _pyproject_document()
    project = document["project"]

    runtime_names = _dependency_names(project["dependencies"])
    dev_names = _dependency_names(project["optional-dependencies"]["dev"])
    build_names = _dependency_names(document["build-system"]["requires"])

    assert not (PROJECT_ROOT / "requirements.lock").exists()
    assert "pip-tools" not in dev_names
    assert {"setuptools", "wheel"} <= build_names
    assert {"pytest", "ruff", "black", "httpx", "build"} <= dev_names
    assert {"colorama", "tzdata"} <= runtime_names


def test_package_discovery_matches_the_src_namespace_used_by_imports():
    document = _pyproject_document()
    setuptools = document["tool"]["setuptools"]

    assert "package-dir" not in setuptools
    assert setuptools["packages"]["find"] == {
        "where": ["."],
        "include": ["src*"],
    }
    assert setuptools["package-data"] == {"src.frontend": ["assets/*.css"]}

    discovered = set(find_namespace_packages(where=PROJECT_ROOT, include=["src*"]))
    assert {"src", "src.backend", "src.frontend"} <= discovered
    assert "backend" not in discovered


def test_iso639_distribution_matches_the_api_used_by_the_application():
    runtime_names = _dependency_names(_pyproject_document()["project"]["dependencies"])

    assert "python-iso639" in runtime_names
    assert "iso639" not in runtime_names
    assert "langcodes" in runtime_names
