from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = PROJECT_ROOT / ".runtime"
RUNTIME_STATE_PATH = RUNTIME_DIR / "appctl.json"
UPDATE_REQUEST_PATH = RUNTIME_DIR / "update-request.json"
LATEST_RELEASE_API_URL = (
    "https://api.github.com/repos/carlunio/media-catalog-books/releases/latest"
)
RELEASE_VERSION_PATTERN = re.compile(
    r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"(?:-(?P<prerelease>[0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)


@dataclass(frozen=True)
class ReleaseStatus:
    current_version: str
    latest_version: str | None = None
    tag_name: str | None = None
    release_url: str | None = None
    available: bool = False
    error: str | None = None


def _version_key(value: str) -> tuple[int, int, int, int] | None:
    match = RELEASE_VERSION_PATTERN.fullmatch(str(value or "").strip())
    if not match:
        return None
    return (
        int(match.group("major")),
        int(match.group("minor")),
        int(match.group("patch")),
        0 if match.group("prerelease") else 1,
    )


def get_latest_release(
    current_version: str,
    *,
    timeout: float = 4.0,
) -> ReleaseStatus:
    current = str(current_version or "").strip()
    try:
        response = requests.get(
            LATEST_RELEASE_API_URL,
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": f"MC-Books/{current or 'unknown'}",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError, TypeError) as exc:
        return ReleaseStatus(
            current_version=current,
            error=f"No se pudo consultar GitHub: {exc}",
        )

    if not isinstance(payload, dict):
        return ReleaseStatus(
            current_version=current,
            error="GitHub devolvió una respuesta de release no válida.",
        )

    tag_name = str(payload.get("tag_name") or "").strip()
    current_key = _version_key(current)
    latest_key = _version_key(tag_name)
    if current_key is None or latest_key is None:
        return ReleaseStatus(
            current_version=current,
            tag_name=tag_name or None,
            error="No se pudo comparar la versión instalada con la release publicada.",
        )

    latest_version = tag_name.removeprefix("v")
    return ReleaseStatus(
        current_version=current,
        latest_version=latest_version,
        tag_name=tag_name,
        release_url=str(payload.get("html_url") or "").strip() or None,
        available=latest_key > current_key,
    )


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def managed_controller() -> dict[str, Any] | None:
    state = _read_json_object(RUNTIME_STATE_PATH)
    try:
        controller_pid = int(state.get("controller_pid") or 0)
    except (TypeError, ValueError):
        controller_pid = 0
    if controller_pid <= 0:
        return None
    return state


def request_managed_update(*, current_version: str, target_version: str) -> None:
    controller = managed_controller()
    if controller is None:
        raise RuntimeError(
            "La aplicación no está gestionada por appctl; ábrela con make start "
            "o con python3 scripts/appctl.py launch."
        )

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "controller_pid": int(controller["controller_pid"]),
        "current_version": str(current_version or "").strip(),
        "target_version": str(target_version or "").strip(),
        "requested_at": datetime.now(UTC).isoformat(),
        "requested_by_pid": os.getpid(),
    }
    temporary = UPDATE_REQUEST_PATH.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(UPDATE_REQUEST_PATH)
