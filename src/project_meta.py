import os
import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"


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
    data = tomllib.loads(PYPROJECT_PATH.read_text(encoding="utf-8"))
    project = data.get("project", {})
    project_name = str(project.get("name") or "media-catalog-books").strip()
    version = str(project.get("version") or "0.0.0").strip()
    channel = str(os.getenv("APP_CHANNEL", "") or "").strip() or None

    return AppMeta(
        project_name=project_name,
        app_name=project_name.replace("-", " ").title(),
        version=version,
        channel=channel,
        changelog_path=CHANGELOG_PATH,
    )
