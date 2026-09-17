import re
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from setuptools import find_namespace_packages

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
LOCK_PATH = PROJECT_ROOT / "requirements.lock"
PIN_PATTERN = re.compile(r"^([A-Za-z0-9_.-]+)==")


def _declared_names() -> set[str]:
    document = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
    requirements = list(document["project"]["dependencies"])
    requirements.extend(document["project"]["optional-dependencies"]["dev"])
    requirements.extend(document["build-system"]["requires"])
    return {canonicalize_name(Requirement(item).name) for item in requirements}


def _locked_names() -> set[str]:
    names = set()
    for line in LOCK_PATH.read_text(encoding="utf-8").splitlines():
        match = PIN_PATTERN.match(line)
        if match:
            names.add(canonicalize_name(match.group(1)))
    return names


def test_lock_contains_all_declared_dependencies_and_hashes():
    lock_text = LOCK_PATH.read_text(encoding="utf-8")

    assert _declared_names() <= _locked_names()
    assert "--hash=sha256:" in lock_text
    assert "/home/" not in lock_text
    assert "file://" not in lock_text


def test_package_discovery_matches_the_src_namespace_used_by_imports():
    document = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
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
    locked_names = _locked_names()

    assert "python-iso639" in locked_names
    assert "iso639" not in locked_names
    assert "language-data" in locked_names


def test_lock_includes_dependencies_needed_only_on_windows():
    locked_names = _locked_names()

    assert {"colorama", "tzdata"} <= locked_names
