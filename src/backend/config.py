import os
import socket
from pathlib import Path


SNAPSHOTS_REPO_DIRNAME = "media-catalog-books"


def _as_float(value: str, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_optional_float(value: str | None, default: float | None = None) -> float | None:
    if value is None:
        return default

    text = str(value).strip()
    if text == "":
        return default

    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return default

    if parsed <= 0:
        return None

    return parsed


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _as_csv_list(value: str | None, default: list[str] | None = None) -> list[str]:
    if value is None:
        source = list(default or [])
    else:
        text = str(value).strip()
        if text == "":
            return []
        source = [chunk.strip() for chunk in text.split(",")]

    items: list[str] = []
    for item in source:
        candidate = str(item).strip()
        if candidate and candidate not in items:
            items.append(candidate)
    return items


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path.cwd())).resolve()


def _resolve_path(env_name: str, default_relative: str) -> Path:
    raw = os.getenv(env_name, default_relative)
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


DB_PATH = _resolve_path("DB_PATH", "data/books.duckdb")
DEFAULT_COVERS_DIR = _resolve_path("COVERS_DIR", "data/input")
DEFAULT_COVERS_OUTPUT_DIR = _resolve_path("COVERS_OUTPUT_DIR", "data/output/covers")
EXPORTS_DIR = _resolve_path("EXPORTS_DIR", "data/output/exports")
OCR_OUTPUT_DIR = _resolve_path("OCR_OUTPUT_DIR", "ocr_output")
BBDD_DIR = _resolve_path("BBDD_DIR", "../bbdd")
CLOUD_SNAPSHOTS_DIR = BBDD_DIR / SNAPSHOTS_REPO_DIRNAME
SYNC_STATE_PATH = _resolve_path("SYNC_STATE_PATH", "data/sync_state.json")
SYNC_ACTOR = os.getenv("SYNC_ACTOR", os.getenv("USER", "usuario")).strip() or "usuario"
SYNC_DEVICE = os.getenv("SYNC_DEVICE", socket.gethostname()).strip() or "equipo"
SYNC_RETENTION_DAYS = _as_int(os.getenv("SYNC_RETENTION_DAYS", "14"), 14)
SYNC_KEEP_MIN = max(1, _as_int(os.getenv("SYNC_KEEP_MIN", "10"), 10))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ISBNDB_API_KEY = os.getenv("ISBNDB_API_KEY")

OCR_PROVIDER = os.getenv("OCR_PROVIDER", "ollama").strip().lower() or "ollama"
OCR_OPENAI_MODEL = os.getenv("OCR_OPENAI_MODEL", os.getenv("OCR_VISION_MODEL", "gpt-4o-mini"))
OCR_OLLAMA_MODEL = os.getenv("OCR_OLLAMA_MODEL", "glm-ocr:latest")
OCR_RESIZE_TO_1800_DEFAULT = _as_bool(os.getenv("OCR_RESIZE_TO_1800_DEFAULT"), True)
OCR_ISBN_OLLAMA_MODEL = os.getenv("OCR_ISBN_OLLAMA_MODEL", "gpt-oss:20b")
OCR_OLLAMA_FALLBACK_MODELS = _as_csv_list(
    os.getenv("OCR_OLLAMA_FALLBACK_MODELS"),
    default=[],
)
OCR_USE_SIDECAR = _as_bool(os.getenv("OCR_USE_SIDECAR"), False)
CATALOG_MODEL = os.getenv("CATALOG_MODEL", "gpt-4o-mini")
CATALOG_OPENAI_MODEL = os.getenv("CATALOG_OPENAI_MODEL", CATALOG_MODEL)
CATALOG_OLLAMA_MODEL = os.getenv("CATALOG_OLLAMA_MODEL", "qwen2.5:14b")
CATALOG_PROVIDER = os.getenv("CATALOG_PROVIDER", "openai").strip().lower() or "openai"
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
OLLAMA_TIMEOUT_SECONDS = _as_optional_float(os.getenv("OLLAMA_TIMEOUT_SECONDS"), None)
CATALOG_ARBITER_ENABLED = _as_bool(os.getenv("CATALOG_ARBITER_ENABLED"), False)
CATALOG_ARBITER_PROVIDER = os.getenv("CATALOG_ARBITER_PROVIDER", "auto").strip().lower() or "auto"
CATALOG_ARBITER_MIN_CONFIDENCE = _as_float(os.getenv("CATALOG_ARBITER_MIN_CONFIDENCE", "0.72"), 0.72)

REQUEST_TIMEOUT_SECONDS = _as_float(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"), 20.0)
WORKFLOW_MAX_ATTEMPTS = _as_int(os.getenv("WORKFLOW_MAX_ATTEMPTS", "2"), 2)
GOOGLE_BOOKS_MIN_INTERVAL_SECONDS = _as_float(os.getenv("GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", "60"), 60.0)
OPENLIBRARY_MIN_INTERVAL_SECONDS = _as_float(os.getenv("OPENLIBRARY_MIN_INTERVAL_SECONDS", "60"), 60.0)

if __name__ == "__main__":
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("DB_PATH:", DB_PATH)
    print("DEFAULT_COVERS_DIR:", DEFAULT_COVERS_DIR)
    print("DEFAULT_COVERS_OUTPUT_DIR:", DEFAULT_COVERS_OUTPUT_DIR)
    print("EXPORTS_DIR:", EXPORTS_DIR)
    print("OCR_OUTPUT_DIR:", OCR_OUTPUT_DIR)
    print("BBDD_DIR:", BBDD_DIR)
    print("CLOUD_SNAPSHOTS_DIR:", CLOUD_SNAPSHOTS_DIR)
    print("SYNC_STATE_PATH:", SYNC_STATE_PATH)
    print("SYNC_ACTOR:", SYNC_ACTOR)
    print("SYNC_DEVICE:", SYNC_DEVICE)
    print("SYNC_RETENTION_DAYS:", SYNC_RETENTION_DAYS)
    print("SYNC_KEEP_MIN:", SYNC_KEEP_MIN)
    print("OCR_PROVIDER:", OCR_PROVIDER)
    print("OCR_OPENAI_MODEL:", OCR_OPENAI_MODEL)
    print("OCR_OLLAMA_MODEL:", OCR_OLLAMA_MODEL)
    print("OCR_RESIZE_TO_1800_DEFAULT:", OCR_RESIZE_TO_1800_DEFAULT)
    print("OCR_ISBN_OLLAMA_MODEL:", OCR_ISBN_OLLAMA_MODEL)
    print("OCR_OLLAMA_FALLBACK_MODELS:", OCR_OLLAMA_FALLBACK_MODELS)
    print("OCR_USE_SIDECAR:", OCR_USE_SIDECAR)
    print("CATALOG_OPENAI_MODEL:", CATALOG_OPENAI_MODEL)
    print("CATALOG_OLLAMA_MODEL:", CATALOG_OLLAMA_MODEL)
    print("CATALOG_PROVIDER:", CATALOG_PROVIDER)
    print("OLLAMA_BASE_URL:", OLLAMA_BASE_URL)
    print("OLLAMA_TIMEOUT_SECONDS:", OLLAMA_TIMEOUT_SECONDS)
    print("CATALOG_ARBITER_ENABLED:", CATALOG_ARBITER_ENABLED)
    print("CATALOG_ARBITER_PROVIDER:", CATALOG_ARBITER_PROVIDER)
    print("CATALOG_ARBITER_MIN_CONFIDENCE:", CATALOG_ARBITER_MIN_CONFIDENCE)
    print("GOOGLE_BOOKS_MIN_INTERVAL_SECONDS:", GOOGLE_BOOKS_MIN_INTERVAL_SECONDS)
    print("OPENLIBRARY_MIN_INTERVAL_SECONDS:", OPENLIBRARY_MIN_INTERVAL_SECONDS)
