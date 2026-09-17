import os
import tomllib
from dataclasses import dataclass
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"
DEFAULT_PROJECT_NAME = "media-catalog-books"


@dataclass(frozen=True)
class AppMeta:
    project_name: str
    app_name: str
    version: str
    channel: str | None
    changelog_path: Path

    @property
    def display_version(self) -> str:
        if self.channel:
            return f"{self.version} ({self.channel})"
        return self.version


@lru_cache(maxsize=1)
def get_app_meta() -> AppMeta:
    try:
        data = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
        project = data.get("project", {})
        project_name = str(project.get("name") or DEFAULT_PROJECT_NAME).strip()
        version = str(project.get("version") or "0.0.0").strip()
    except (OSError, tomllib.TOMLDecodeError):
        try:
            installed = distribution(DEFAULT_PROJECT_NAME)
        except PackageNotFoundError:
            project_name = DEFAULT_PROJECT_NAME
            version = "0.0.0"
        else:
            project_name = str(installed.metadata.get("Name") or DEFAULT_PROJECT_NAME)
            version = str(installed.version or "0.0.0")
    channel = str(os.getenv("APP_CHANNEL", "") or "").strip() or None

    return AppMeta(
        project_name=project_name,
        app_name=project_name.replace("-", " ").title(),
        version=version,
        channel=channel,
        changelog_path=CHANGELOG_PATH,
    )
