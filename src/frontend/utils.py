import os
import re
import time
import html
from pathlib import Path
from typing import Any

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://127.0.0.1:8000")
WORKFLOW_STAGES = ("ocr", "metadata", "catalog", "cover")
BLOCK_OPTIONS = ("A", "B", "C")
MODULE_NAME_PATTERN = re.compile(r"^\d{2}$")

GLOBAL_SELECTED_BOOK_KEY = "global_selected_book_id"
GLOBAL_SELECTED_BLOCK_KEY = "global_selected_block"
GLOBAL_SELECTED_MODULE_KEY = "global_selected_module"
THEME_APPLIED_KEY = "_ui_theme_applied"
THEME_CSS_ENV_VAR = "FRONTEND_THEME_CSS"
DEFAULT_THEME_CSS_PATH = Path(__file__).resolve().parent / "assets" / "theme.css"
APP_ICON_PATH = Path(__file__).resolve().parents[2] / "assets" / "dani.png"


def _normalize_provider(value: str | None, *, fallback: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"openai", "ollama"}:
        return text
    return fallback


def _as_csv_models(value: str | None, *, default: list[str]) -> list[str]:
    text = str(value or "").strip()
    source = [chunk.strip() for chunk in text.split(",")] if text else list(default)
    output: list[str] = []
    for item in source:
        candidate = str(item or "").strip()
        if candidate and candidate not in output:
            output.append(candidate)
    return output


def _as_bool(value: str | None, *, fallback: bool) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return fallback


OCR_PROVIDER_DEFAULT = _normalize_provider(os.getenv("OCR_PROVIDER"), fallback="ollama")
OCR_OPENAI_MODEL_DEFAULT = str(os.getenv("OCR_OPENAI_MODEL", "gpt-4o-mini") or "gpt-4o-mini").strip()
OCR_OLLAMA_MODEL_DEFAULT = str(os.getenv("OCR_OLLAMA_MODEL", "glm-ocr:latest") or "glm-ocr:latest").strip()
OCR_RESIZE_TO_1800_DEFAULT = _as_bool(os.getenv("OCR_RESIZE_TO_1800_DEFAULT"), fallback=True)

CATALOG_PROVIDER_DEFAULT = _normalize_provider(os.getenv("CATALOG_PROVIDER"), fallback="openai")
CATALOG_MODEL_DEFAULT = str(os.getenv("CATALOG_MODEL", "gpt-4o-mini") or "gpt-4o-mini").strip()
CATALOG_OPENAI_MODEL_DEFAULT = str(os.getenv("CATALOG_OPENAI_MODEL", CATALOG_MODEL_DEFAULT) or CATALOG_MODEL_DEFAULT).strip()
CATALOG_OLLAMA_MODEL_DEFAULT = str(os.getenv("CATALOG_OLLAMA_MODEL", "qwen2.5:14b") or "qwen2.5:14b").strip()

OCR_OLLAMA_MODEL_SUGGESTIONS = _as_csv_models(
    os.getenv("OCR_OLLAMA_MODEL_SUGGESTIONS"),
    default=[OCR_OLLAMA_MODEL_DEFAULT, "glm-ocr:latest", "glm-ocr"],
)
CATALOG_OLLAMA_MODEL_SUGGESTIONS = _as_csv_models(
    os.getenv("CATALOG_OLLAMA_MODEL_SUGGESTIONS"),
    default=[CATALOG_OLLAMA_MODEL_DEFAULT, "qwen2.5:14b", "qwen3:14b", "qwen2.5:7b", "llama3.1:8b"],
)


def seed_widget_once(key: str, value: Any) -> None:
    marker = f"__seeded__{key}"
    marker_value = f"__seeded_value__{key}"

    has_key = key in st.session_state
    current_value = st.session_state.get(key)
    previous_seed = st.session_state.get(marker_value)

    should_seed = False
    if not has_key:
        should_seed = True
    elif previous_seed is None:
        # Backward-compatible migration from older sessions where only a boolean
        # marker existed (or no marker value was tracked).
        should_seed = True
    elif current_value == previous_seed:
        # Keep defaults in sync with .env while the user has not changed the widget.
        should_seed = True

    if should_seed:
        st.session_state[key] = value

    st.session_state[marker_value] = value
    st.session_state[marker] = True


def _resolve_theme_css_path() -> Path:
    raw = str(os.getenv(THEME_CSS_ENV_VAR, "") or "").strip()
    if not raw:
        return DEFAULT_THEME_CSS_PATH

    path = Path(raw).expanduser()
    if path.is_absolute():
        return path

    candidate_in_assets = DEFAULT_THEME_CSS_PATH.parent / path
    if candidate_in_assets.exists():
        return candidate_in_assets

    return (Path.cwd() / path).resolve()


def _load_theme_css() -> str:
    css_path = _resolve_theme_css_path()
    try:
        return css_path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _apply_theme() -> None:
    already = bool(st.session_state.get(THEME_APPLIED_KEY, False))
    css = _load_theme_css()
    if css:
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
    if not already:
        st.session_state[THEME_APPLIED_KEY] = True


def configure_page(title: str = "Media Catalog Books") -> None:
    page_icon = str(APP_ICON_PATH) if APP_ICON_PATH.exists() else None
    try:
        if page_icon:
            st.set_page_config(page_title=title, layout="wide", page_icon=page_icon)
        else:
            st.set_page_config(page_title=title, layout="wide")
    except Exception:
        pass
    _apply_theme()


def _url(path: str) -> str:
    return f"{API_URL}{path}"


def _covers_root() -> Path:
    raw = os.getenv("COVERS_DIR", "data/input")
    path = Path(raw)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def list_existing_modules(block: str) -> list[str]:
    block_text = str(block or "").strip().upper()
    if block_text not in BLOCK_OPTIONS:
        return []

    block_dir = _covers_root() / block_text
    if not block_dir.exists() or not block_dir.is_dir():
        return []

    modules: list[str] = []
    for child in sorted(block_dir.iterdir()):
        if child.name.startswith("."):
            continue
        if child.is_dir() and MODULE_NAME_PATTERN.fullmatch(child.name):
            modules.append(child.name)

    return modules


def api_get(path: str, *, timeout: float | None = 120.0, **kwargs) -> Any:
    response = requests.get(_url(path), timeout=timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_get_bytes(path: str, *, timeout: float | None = 120.0, **kwargs) -> bytes:
    response = requests.get(_url(path), timeout=timeout, **kwargs)
    response.raise_for_status()
    return response.content


def api_post(path: str, *, timeout: float | None = 120.0, **kwargs) -> Any:
    response = requests.post(_url(path), timeout=timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def api_put(path: str, *, timeout: float | None = 120.0, **kwargs) -> Any:
    response = requests.put(_url(path), timeout=timeout, **kwargs)
    response.raise_for_status()
    return response.json()


def show_backend_status() -> None:
    last_exc: Exception | None = None
    for attempt in range(12):
        try:
            api_get("/health", timeout=3.0)
            st.success(f"Backend activo: {API_URL}")
            return
        except Exception as exc:
            last_exc = exc
            if attempt < 11:
                time.sleep(0.4)

    st.error(f"Backend no disponible: {API_URL} ({last_exc})")


def load_stats(*, block: str | None = None, module: str | None = None) -> dict[str, int]:
    try:
        params: dict[str, Any] = {}
        if block and module:
            params["block"] = block
            params["module"] = module

        payload = api_get("/stats", params=params, timeout=8.0)
        if isinstance(payload, dict):
            return {key: int(value) for key, value in payload.items() if isinstance(value, (int, float))}
    except Exception:
        pass

    return {
        "total": 0,
        "needs_ocr": 0,
        "needs_metadata": 0,
        "needs_catalog": 0,
        "needs_cover": 0,
        "needs_workflow_review": 0,
    }


def get_selected_book_id() -> str | None:
    value = st.session_state.get(GLOBAL_SELECTED_BOOK_KEY)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def set_selected_book_id(book_id: str | None) -> None:
    text = str(book_id or "").strip()
    if text:
        st.session_state[GLOBAL_SELECTED_BOOK_KEY] = text


def get_selected_scope() -> tuple[str, str | None]:
    block = str(st.session_state.get(GLOBAL_SELECTED_BLOCK_KEY, "A") or "A").strip().upper()
    if block not in BLOCK_OPTIONS:
        block = "A"

    available_modules = list_existing_modules(block)

    raw_module = st.session_state.get(GLOBAL_SELECTED_MODULE_KEY)
    module_text = str(raw_module or "").strip().zfill(2) if raw_module else ""

    module: str | None
    if module_text and module_text in available_modules:
        module = module_text
    elif available_modules:
        module = available_modules[0]
    else:
        module = None

    st.session_state[GLOBAL_SELECTED_BLOCK_KEY] = block
    st.session_state[GLOBAL_SELECTED_MODULE_KEY] = module or ""
    return block, module


def set_selected_scope(block: str, module: str | None) -> tuple[str, str | None]:
    block_text = str(block or "A").strip().upper()
    if block_text not in BLOCK_OPTIONS:
        block_text = "A"

    available_modules = list_existing_modules(block_text)

    module_text = str(module or "").strip().zfill(2) if module else ""
    chosen_module: str | None
    if module_text and module_text in available_modules:
        chosen_module = module_text
    elif available_modules:
        chosen_module = available_modules[0]
    else:
        chosen_module = None

    st.session_state[GLOBAL_SELECTED_BLOCK_KEY] = block_text
    st.session_state[GLOBAL_SELECTED_MODULE_KEY] = chosen_module or ""
    return block_text, chosen_module


def scope_params(block: str, module: str | None) -> dict[str, str]:
    module_text = str(module or "").strip()
    if not module_text:
        return {}
    return {"block": str(block).strip().upper(), "module": module_text.zfill(2)}


def select_module_scope(*, key_prefix: str, title: str = "Módulo activo") -> tuple[str, str | None]:
    current_block, current_module = get_selected_scope()

    st.caption(title)
    col_block, col_module = st.columns([1, 1])
    with col_block:
        block = st.selectbox(
            "Bloque",
            BLOCK_OPTIONS,
            index=BLOCK_OPTIONS.index(current_block),
            key=f"{key_prefix}_block",
        )

    available_modules = list_existing_modules(block)

    with col_module:
        if available_modules:
            default_module = current_module if current_module in available_modules else available_modules[0]
            module = st.selectbox(
                "Módulo",
                available_modules,
                index=available_modules.index(default_module),
                key=f"{key_prefix}_module",
            )
        else:
            module = None
            st.caption("Sin módulos disponibles")

    selected_block, selected_module = set_selected_scope(block, module)

    if not selected_module:
        st.warning(
            f"No hay carpetas de módulo (01..99) para el bloque {selected_block} en "
            f"{_covers_root() / selected_block}."
        )

    return selected_block, selected_module


def select_book_id(rows: list[dict[str, Any]], *, label: str, key: str) -> str:
    ids = [str(row.get("id") or "").strip() for row in rows]
    ids = [item for item in ids if item]
    if not ids:
        raise ValueError("No hay IDs disponibles")

    labels = {}
    for row in rows:
        book_id = str(row.get("id") or "").strip()
        if not book_id:
            continue
        title = str((row.get("catalog") or {}).get("titulo") or "").strip() or "(sin titulo)"
        stage = str(row.get("pipeline_stage") or "unknown")
        review = " | review" if bool(row.get("workflow_needs_review")) else ""
        block = str(row.get("block") or "").strip()
        module = str(row.get("module") or "").strip()
        scope = f"{block}/{module}" if block and module else "--"
        labels[book_id] = f"{book_id} | {scope} | {title} | {stage}{review}"

    preferred = get_selected_book_id()
    index = ids.index(preferred) if preferred in ids else 0
    selected = st.selectbox(label, ids, index=index, key=key, format_func=lambda value: labels.get(value, value))
    set_selected_book_id(selected)
    return selected


@st.cache_data(ttl=30)
def load_ollama_models() -> list[str]:
    payload = api_get("/models/ollama", timeout=6.0)
    if not isinstance(payload, dict):
        return []

    models = payload.get("models", [])
    if not isinstance(models, list):
        return []

    cleaned = [str(item).strip() for item in models if str(item).strip()]
    return sorted(set(cleaned))


def render_ollama_model_selector(
    *,
    label: str,
    key: str,
    installed_models: list[str],
    default_model: str,
    suggested_models: list[str] | None = None,
    disabled: bool = False,
) -> str:
    options = [str(item).strip() for item in installed_models if str(item).strip()]
    options = sorted(set(options))

    default_text = str(default_model or "").strip()
    selected = default_text

    def _match_default(value: str, candidates: list[str]) -> str | None:
        if not value:
            return None
        if value in candidates:
            return value
        lower_map = {item.lower(): item for item in candidates}
        if value.lower() in lower_map:
            return lower_map[value.lower()]

        # Common OCR/Catalog aliasing: `model` <-> `model:latest`
        if ":" not in value:
            latest = f"{value}:latest"
            if latest in candidates:
                return latest
            if latest.lower() in lower_map:
                return lower_map[latest.lower()]
        if value.endswith(":latest"):
            base = value[: -len(":latest")]
            if base in candidates:
                return base
            if base.lower() in lower_map:
                return lower_map[base.lower()]
        return None

    if options:
        matched_default = _match_default(default_text, options)
        display_options = list(options)
        if default_text and matched_default is None and default_text not in display_options:
            # Keep .env value visible and selected even if not installed.
            display_options = [default_text, *display_options]

        preferred = matched_default or default_text or (display_options[0] if display_options else "")
        if preferred and preferred not in display_options:
            preferred = display_options[0]

        seed_widget_once(key, preferred)
        current_value = str(st.session_state.get(key) or "").strip()
        if current_value not in display_options:
            st.session_state[key] = preferred
        selected = str(st.selectbox(label, display_options, key=key, disabled=disabled) or "").strip()
        if default_text and selected == default_text and matched_default is None:
            st.caption(
                f"Modelo desde `.env`: `{default_text}` (no detectado como instalado en backend). "
                "Si ejecutas sin tocar nada, se usará este valor."
            )
    else:
        seed_widget_once(key, "(sin modelos Ollama instalados en backend)")
        st.selectbox(
            label,
            options=["(sin modelos Ollama instalados en backend)"],
            index=0,
            key=key,
            disabled=True,
        )
        st.caption("No hay modelos Ollama instalados detectables en el backend.")

    suggestions = [str(item).strip() for item in (suggested_models or []) if str(item).strip()]
    if suggestions:
        ordered: list[str] = []
        for item in suggestions:
            if item not in ordered:
                ordered.append(item)
        installed_set = set(options)

        chips: list[str] = []
        for model_name in ordered:
            escaped = html.escape(model_name)
            if model_name in installed_set:
                chips.append(
                    "<span style='display:inline-block;padding:0.15rem 0.5rem;border:1px solid #5b7280;"
                    "border-radius:999px;background:#eaf4fb;color:#29414e;font-size:0.82rem;'>"
                    f"{escaped}</span>"
                )
            else:
                chips.append(
                    "<span style='display:inline-block;padding:0.15rem 0.5rem;border:1px solid #9daab2;"
                    "border-radius:999px;background:#eef1f3;color:#7a8892;font-size:0.82rem;opacity:0.65;"
                    "cursor:not-allowed;'>"
                    f"{escaped}</span>"
                )

        st.caption("Sugerencias Ollama (gris = no instalado, no seleccionable)")
        st.markdown(
            "<div style='display:flex;flex-wrap:wrap;gap:0.35rem 0.4rem;align-items:center;'>"
            + "".join(chips)
            + "</div>",
            unsafe_allow_html=True,
        )

    return selected
