import json
import re
from pathlib import Path

import streamlit as st
from PIL import Image, ImageOps

try:
    from src.frontend.utils import (
        api_get,
        api_post,
        api_put,
        configure_page,
        render_page_header,
        CATALOG_OLLAMA_MODEL_DEFAULT,
        CATALOG_OLLAMA_MODEL_SUGGESTIONS,
        CATALOG_OPENAI_MODEL_DEFAULT,
        CATALOG_PROVIDER_DEFAULT,
        load_ollama_models,
        OCR_OLLAMA_MODEL_DEFAULT,
        OCR_OLLAMA_MODEL_SUGGESTIONS,
        OCR_OPENAI_MODEL_DEFAULT,
        OCR_PROVIDER_DEFAULT,
        OCR_RESIZE_TO_1800_DEFAULT,
        render_ollama_model_selector,
        render_openai_catalog_model_selector,
        render_openai_ocr_model_selector,
        seed_widget_once,
        scope_params,
        select_book_id,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        api_get,
        api_post,
        api_put,
        configure_page,
        render_page_header,
        CATALOG_OLLAMA_MODEL_DEFAULT,
        CATALOG_OLLAMA_MODEL_SUGGESTIONS,
        CATALOG_OPENAI_MODEL_DEFAULT,
        CATALOG_PROVIDER_DEFAULT,
        load_ollama_models,
        OCR_OLLAMA_MODEL_DEFAULT,
        OCR_OLLAMA_MODEL_SUGGESTIONS,
        OCR_OPENAI_MODEL_DEFAULT,
        OCR_PROVIDER_DEFAULT,
        OCR_RESIZE_TO_1800_DEFAULT,
        render_ollama_model_selector,
        render_openai_catalog_model_selector,
        render_openai_ocr_model_selector,
        seed_widget_once,
        scope_params,
        select_book_id,
        select_module_scope,
        show_backend_status,
    )

ISBN_CANDIDATE_PATTERN = re.compile(r"[0-9XxIiLlOo\- ]{9,}")
STAGE_ORDER = ("ocr", "metadata", "catalog", "cover")
STAGE_LABELS = {
    "ocr": "OCR",
    "metadata": "Metadatos",
    "catalog": "Catalogación",
    "cover": "Portada",
}

configure_page("Revisión OCR | MC Books")
render_page_header(
    phase="2",
    title="Revisión OCR e ISBN",
    description=(
        "Comprueba el texto reconocido y el ISBN, y decide cómo debe continuar "
        "cada ficha pendiente."
    ),
)
show_backend_status()

flash_success = str(st.session_state.pop("book_review_flash_success", "") or "").strip()
if flash_success:
    st.success(flash_success)


def _load_image_with_orientation(path: str):
    with Image.open(path) as image:
        return ImageOps.exif_transpose(image).copy()


def _clean_isbn(raw: str | None) -> str:
    return str(raw or "").strip().replace("-", "").replace(" ", "").upper()


def _is_valid_isbn(raw: str | None) -> bool:
    isbn = _clean_isbn(raw)
    if re.fullmatch(r"\d{9}[0-9X]", isbn):
        total = 0
        for index, char in enumerate(isbn):
            value = 10 if char == "X" else int(char)
            total += (10 - index) * value
        return total % 11 == 0

    if re.fullmatch(r"\d{13}", isbn):
        total = 0
        for index, char in enumerate(isbn):
            factor = 1 if index % 2 == 0 else 3
            total += int(char) * factor
        return total % 10 == 0

    return False


def _normalize_ocular_isbn_confusions(text: str) -> str:
    return str(text or "").upper().replace("I", "1").replace("L", "1").replace("O", "0")


def _isbn_candidate_detail(raw: str | None) -> dict:
    cleaned = _clean_isbn(raw)
    detail = {
        "raw": str(raw or ""),
        "cleaned": cleaned,
        "kind": "unknown",
        "valid": False,
        "reason": "invalid_format",
    }

    if re.fullmatch(r"\d{9}[0-9X]", cleaned):
        detail["kind"] = "isbn10"
        if _is_valid_isbn(cleaned):
            detail["valid"] = True
            detail["reason"] = "valid_isbn10_checksum"
        else:
            detail["reason"] = "invalid_isbn10_checksum"
        return detail

    if re.fullmatch(r"\d{13}", cleaned):
        detail["kind"] = "isbn13"
        if _is_valid_isbn(cleaned):
            detail["valid"] = True
            detail["reason"] = "valid_isbn13_checksum"
        else:
            detail["reason"] = "invalid_isbn13_checksum"
        return detail

    detail["reason"] = "invalid_length_or_chars"
    return detail


def _reason_label(code: str | None) -> str:
    mapping = {
        "valid_isbn10_checksum": "ISBN-10 válido (suma de comprobación)",
        "valid_isbn13_checksum": "ISBN-13 válido (suma de comprobación)",
        "invalid_isbn10_checksum": "ISBN-10 inválido (suma de comprobación)",
        "invalid_isbn13_checksum": "ISBN-13 inválido (suma de comprobación)",
        "invalid_length_or_chars": "Formato inválido (longitud o caracteres)",
        "invalid_format": "Formato inválido",
    }
    key = str(code or "").strip()
    return mapping.get(key, key or "desconocido")


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if candidate and candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def _derive_isbn_from_text(text: str | None) -> dict:
    body = str(text or "")
    raw_candidates = [
        _clean_isbn(match) for match in ISBN_CANDIDATE_PATTERN.findall(body)
    ]
    raw_candidates = _unique([value for value in raw_candidates if value])

    normalized_candidates = _unique(
        [_normalize_ocular_isbn_confusions(value) for value in raw_candidates]
    )
    valid_candidates = _unique(
        [value for value in raw_candidates if _is_valid_isbn(value)]
    )
    normalized_valid_candidates = _unique(
        [value for value in normalized_candidates if _is_valid_isbn(value)]
    )
    candidate_details = [_isbn_candidate_detail(value) for value in raw_candidates]
    normalized_candidate_details = [
        _isbn_candidate_detail(value) for value in normalized_candidates
    ]

    selected = None
    selected_source = None
    if valid_candidates:
        selected = valid_candidates[0]
        selected_source = "checksum_direct"
    elif normalized_valid_candidates:
        selected = normalized_valid_candidates[0]
        selected_source = "checksum_after_ocr_normalization"

    return {
        "isbn_raw": raw_candidates[0] if raw_candidates else None,
        "isbn": selected,
        "is_valid": bool(selected),
        "source": selected_source,
        "raw_candidates": raw_candidates,
        "normalized_candidates": normalized_candidates,
        "valid_candidates": valid_candidates,
        "normalized_valid_candidates": normalized_valid_candidates,
        "candidate_details": candidate_details,
        "normalized_candidate_details": normalized_candidate_details,
    }


def _is_ocr_review(row: dict) -> bool:
    if not bool(row.get("workflow_needs_review")):
        return False

    node = str(row.get("workflow_current_node") or "").strip().lower()
    reason = str(row.get("workflow_review_reason") or "").strip().lower()
    return node in {"ocr", "ocr_isbn_validation"} or "ocr" in reason or "isbn" in reason


def _filter_rows(rows: list[dict], mode: str) -> list[dict]:
    if mode == "review":
        return [row for row in rows if bool(row.get("workflow_needs_review"))]
    if mode == "review_ocr":
        return [row for row in rows if _is_ocr_review(row)]
    return rows


def _review_origin_stage(book: dict) -> str | None:
    node = str(book.get("workflow_current_node") or "").strip().lower()
    reason = str(book.get("workflow_review_reason") or "").strip().lower()

    if node.startswith("stage:"):
        candidate = node.split(":", maxsplit=1)[1].strip()
        if candidate in STAGE_ORDER:
            return candidate
    if node.startswith("retry_"):
        candidate = node[len("retry_") :].strip()
        if candidate in STAGE_ORDER:
            return candidate

    for stage in STAGE_ORDER:
        if (
            node == stage
            or node.startswith(f"{stage}_")
            or node.startswith(f"{stage}:")
        ):
            return stage

    for stage in STAGE_ORDER:
        if reason.startswith(stage) or f"stage:{stage}" in reason:
            return stage

    if "isbn" in reason or "ocr" in reason:
        return "ocr"
    return None


def _retry_stage_options(book: dict, *, origin_stage: str | None = None) -> list[str]:
    ocr_status = str(book.get("ocr_status") or "").strip().lower()
    metadata_status = str(book.get("metadata_status") or "").strip().lower()
    catalog_status = str(book.get("catalog_status") or "").strip().lower()
    cover_status = str(book.get("cover_status") or "").strip().lower()

    max_index = 0
    if ocr_status in {"processed", "manual", "skipped"}:
        max_index = 0
    if metadata_status in {"fetched", "partial", "manual", "skipped"}:
        max_index = 1
    if catalog_status in {"built", "partial", "manual"}:
        max_index = 2
    if cover_status in {"downloaded", "manual", "skipped"}:
        max_index = 3

    if origin_stage in STAGE_ORDER:
        max_index = max(max_index, STAGE_ORDER.index(str(origin_stage)))

    return list(STAGE_ORDER[: max_index + 1])


def _sync_form_defaults(book_id: str, book: dict) -> tuple[str, str, str]:
    credits_key = f"ocr_review_credits_{book_id}"
    isbn_raw_key = f"ocr_review_isbn_raw_{book_id}"
    isbn_key = f"ocr_review_isbn_{book_id}"
    pending_key = f"ocr_review_pending_patch_{book_id}"

    if credits_key not in st.session_state:
        st.session_state[credits_key] = str(book.get("credits_text") or "")
    if isbn_raw_key not in st.session_state:
        st.session_state[isbn_raw_key] = str(book.get("isbn_raw") or "")
    if isbn_key not in st.session_state:
        st.session_state[isbn_key] = str(book.get("isbn") or "")

    pending = st.session_state.pop(pending_key, None)
    if isinstance(pending, dict):
        if "credits_text" in pending:
            st.session_state[credits_key] = str(pending.get("credits_text") or "")
        if "isbn_raw" in pending:
            st.session_state[isbn_raw_key] = str(pending.get("isbn_raw") or "")
        if "isbn" in pending:
            st.session_state[isbn_key] = str(pending.get("isbn") or "")

    return credits_key, isbn_raw_key, isbn_key


def _queue_form_patch(
    book_id: str,
    *,
    credits_text: str | None = None,
    isbn_raw: str | None = None,
    isbn: str | None = None,
) -> None:
    patch: dict[str, str] = {}
    if credits_text is not None:
        patch["credits_text"] = str(credits_text)
    if isbn_raw is not None:
        patch["isbn_raw"] = str(isbn_raw)
    if isbn is not None:
        patch["isbn"] = str(isbn)
    if patch:
        st.session_state[f"ocr_review_pending_patch_{book_id}"] = patch


scope_block, scope_module = select_module_scope(
    key_prefix="review_scope", title="Módulo de trabajo"
)
if not scope_module:
    st.stop()

st.subheader("Libro que vas a revisar")
st.caption("Filtra la cola y recorre los registros del módulo seleccionado.")

mode_labels = {
    "all": "Mostrar todos",
    "review": "Solo en revisión",
    "review_ocr": "Solo revisión de OCR/ISBN",
}
if "book_review_filter_mode" not in st.session_state:
    st.session_state["book_review_filter_mode"] = "all"

filter_col, limit_col = st.columns([3, 1])
with filter_col:
    filter_mode = st.segmented_control(
        "Libros que se mostrarán",
        options=list(mode_labels.keys()),
        format_func=lambda value: mode_labels.get(str(value), str(value)),
        key="book_review_filter_mode",
        width="stretch",
    )
with limit_col:
    limit = st.number_input(
        "Límite de resultados",
        min_value=1,
        max_value=5000,
        value=1200,
    )
if filter_mode is None:
    filter_mode = "all"

try:
    rows = api_get(
        "/books",
        params={"limit": int(limit), **scope_params(scope_block, scope_module)},
        timeout=20.0,
    )
except Exception as exc:
    st.error(f"No se pudo cargar la lista de libros: {exc}")
    st.stop()

if not rows:
    st.info("No hay libros para el módulo seleccionado.")
    st.stop()

filtered_rows = _filter_rows(rows, str(filter_mode))
st.caption(
    f"Filtro actual: {mode_labels.get(str(filter_mode), 'Mostrar todos')} | "
    f"{len(filtered_rows)} de {len(rows)} libros"
)

if not filtered_rows:
    st.info("No hay libros con los filtros actuales.")
    st.stop()

book_ids = [
    str(row.get("id") or "") for row in filtered_rows if str(row.get("id") or "")
]
selector_key = "book_ocr_review_selector"
selector_pending_key = "book_ocr_review_selector_pending"

pending_selected = st.session_state.pop(selector_pending_key, None)
if pending_selected in book_ids:
    st.session_state[selector_key] = pending_selected

if (
    selector_key in st.session_state
    and st.session_state.get(selector_key) not in book_ids
):
    st.session_state[selector_key] = book_ids[0]

selected_id = select_book_id(filtered_rows, label="Selecciona libro", key=selector_key)
current_index = book_ids.index(selected_id)

col_prev, col_next = st.columns(2)
with col_prev:
    if st.button(
        "← Anterior",
        disabled=current_index == 0,
        key="book_ocr_review_prev",
        width="stretch",
    ):
        st.session_state[selector_pending_key] = book_ids[current_index - 1]
        st.rerun()
with col_next:
    if st.button(
        "Siguiente →",
        disabled=current_index == len(book_ids) - 1,
        key="book_ocr_review_next",
        width="stretch",
    ):
        st.session_state[selector_pending_key] = book_ids[current_index + 1]
        st.rerun()

st.caption(f"Registro {current_index + 1} de {len(book_ids)}")

try:
    book = api_get(f"/books/{selected_id}", timeout=20.0)
except Exception as exc:
    st.error(f"No se pudo cargar el libro {selected_id}: {exc}")
    st.stop()

credits_key, isbn_raw_key, isbn_key = _sync_form_defaults(selected_id, book)
validation_from_text = _derive_isbn_from_text(st.session_state.get(credits_key, ""))

left, right = st.columns([1, 2])

with left:
    st.markdown("### Imagen")
    image_path = str(book.get("image_path") or "").strip()
    if image_path and Path(image_path).exists():
        try:
            st.image(_load_image_with_orientation(image_path), width="stretch")
            st.caption(Path(image_path).name)
        except (FileNotFoundError, OSError) as exc:
            st.warning(f"No se pudo cargar la imagen: {exc}")
    else:
        st.info("No hay imagen local accesible")

    st.markdown("### Estado")
    with st.container(border=True):
        pipeline_stage = str(book.get("pipeline_stage") or "—")
        workflow_status = str(book.get("workflow_status") or "—")
        ocr_status = str(book.get("ocr_status") or "—")
        provider = str(book.get("ocr_provider") or "—")
        model = str(book.get("ocr_model") or "—")
        st.markdown(f"**Libro:** {book.get('id') or selected_id}")
        st.caption(
            f"Etapa: {STAGE_LABELS.get(pipeline_stage, pipeline_stage)} · "
            f"Flujo: {workflow_status} · OCR: {ocr_status}"
        )
        st.caption(f"Modelo OCR: {provider} / {model}")
        review_detail = str(book.get("workflow_review_reason") or "").strip()
        if review_detail:
            st.markdown("**Motivo de revisión**")
            st.caption(review_detail)

with right:
    st.markdown("### OCR + ISBN")

    metrics_a, metrics_b, metrics_c = st.columns(3)
    metrics_a.metric("ISBN validado", "sí" if bool(book.get("isbn")) else "no")
    metrics_b.metric(
        "Candidatos detectados", len(validation_from_text.get("raw_candidates") or [])
    )
    metrics_c.metric(
        "Candidatos válidos", len(validation_from_text.get("valid_candidates") or [])
    )

    if validation_from_text.get("isbn"):
        st.success(
            f"ISBN sugerido por reglas: {validation_from_text.get('isbn')} "
            f"({validation_from_text.get('source')})"
        )
    elif st.session_state.get(credits_key, "").strip():
        st.warning(
            "No hay ningún ISBN válido detectado en el texto OCR con las reglas actuales."
        )

    manual_isbn_input = str(st.session_state.get(isbn_key, "")).strip()
    manual_raw_input = str(st.session_state.get(isbn_raw_key, "")).strip()
    manual_candidate = manual_isbn_input or manual_raw_input
    if manual_candidate:
        manual_detail = _isbn_candidate_detail(manual_candidate)
        if bool(manual_detail.get("valid")):
            st.info(f"ISBN manual actual válido: {manual_detail.get('cleaned')}")
        else:
            st.warning(
                "ISBN manual actual inválido: "
                f"{manual_detail.get('cleaned') or manual_detail.get('raw')} "
                f"({ _reason_label(manual_detail.get('reason')) })"
            )

    candidate_details = (
        validation_from_text.get("candidate_details")
        if isinstance(validation_from_text.get("candidate_details"), list)
        else []
    )
    if candidate_details:
        table_rows = [
            {
                "raw": str(item.get("raw") or ""),
                "cleaned": str(item.get("cleaned") or ""),
                "tipo": str(item.get("kind") or "unknown"),
                "válido": "sí" if bool(item.get("valid")) else "no",
                "motivo": _reason_label(item.get("reason")),
            }
            for item in candidate_details
        ]
        st.markdown("Candidatos ISBN detectados")
        st.dataframe(table_rows, hide_index=True, width="stretch")

    with st.expander("Detalle de validación del ISBN", expanded=False):
        st.json(validation_from_text)

    with st.form(f"ocr_review_form_{selected_id}"):
        st.text_area(
            "Texto OCR (credits_text)",
            key=credits_key,
            height=340,
        )
        st.text_input("ISBN raw", key=isbn_raw_key)
        st.text_input("ISBN validado", key=isbn_key)

        form_col_a, form_col_b, form_col_c = st.columns(3)
        with form_col_a:
            suggest = st.form_submit_button("Extraer ISBN desde texto")
        with form_col_b:
            save = st.form_submit_button("Guardar OCR revisado")
        with form_col_c:
            if _is_ocr_review(book):
                save_and_leave_review = st.form_submit_button(
                    "Guardar ISBN válido y salir de revisión",
                    type="primary",
                    help=(
                        "Valida la suma de comprobación, guarda el OCR y el ISBN, "
                        "y deja el libro preparado para continuar con metadatos."
                    ),
                )
                consolidate_isbn = False
            else:
                consolidate_isbn = st.form_submit_button(
                    "Guardar ISBN en la base de datos",
                    type="primary",
                )
                save_and_leave_review = False

    if suggest:
        suggested = _derive_isbn_from_text(st.session_state.get(credits_key, ""))
        _queue_form_patch(
            selected_id,
            isbn_raw=str(suggested.get("isbn_raw") or ""),
            isbn=str(suggested.get("isbn") or ""),
        )
        st.rerun()

    if save:
        payload = {
            "credits_text": str(st.session_state.get(credits_key, "")).strip() or None,
            "isbn_raw": str(st.session_state.get(isbn_raw_key, "")).strip() or None,
            "isbn": str(st.session_state.get(isbn_key, "")).strip() or None,
        }
        try:
            result = api_put(f"/books/{selected_id}/ocr", json=payload, timeout=20.0)
            if isinstance(result, dict):
                isbn_value = str(result.get("isbn") or "").strip()
                isbn_raw_value = str(result.get("isbn_raw") or "").strip()
                _queue_form_patch(selected_id, isbn_raw=isbn_raw_value, isbn=isbn_value)
                if isbn_value:
                    st.success(f"ISBN final validado: {isbn_value}")
                else:
                    st.warning(
                        "No se pudo validar ISBN final (se guardó el OCR y ISBN raw)."
                    )
                with st.expander("Resultado de guardado", expanded=False):
                    st.json(result)
            if _is_ocr_review(book):
                st.session_state["book_review_flash_success"] = (
                    "OCR actualizado. El libro sigue en revisión; usa "
                    "«Guardar ISBN válido y salir de revisión» para completarla."
                )
            else:
                st.session_state["book_review_flash_success"] = "OCR actualizado"
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo actualizar OCR: {exc}")

    if save_and_leave_review:
        manual_candidate = (
            str(st.session_state.get(isbn_key, "")).strip()
            or str(st.session_state.get(isbn_raw_key, "")).strip()
        )
        if not _is_valid_isbn(manual_candidate):
            st.error(
                "No se puede salir de revisión: introduce un ISBN-10 o ISBN-13 "
                "con una suma de comprobación válida."
            )
        else:
            payload = {
                "credits_text": (
                    str(st.session_state.get(credits_key, "")).strip() or None
                ),
                "isbn_raw": (
                    str(st.session_state.get(isbn_raw_key, "")).strip() or None
                ),
                "isbn": str(st.session_state.get(isbn_key, "")).strip() or None,
            }
            try:
                result = api_put(
                    f"/books/{selected_id}/ocr",
                    json=payload,
                    timeout=20.0,
                )
                if not isinstance(result, dict) or not bool(result.get("isbn_valid")):
                    st.error(
                        "El backend no ha validado el ISBN. El libro continúa en revisión."
                    )
                else:
                    api_post(
                        f"/workflow/review/{selected_id}",
                        json={"action": "approve"},
                        timeout=60.0,
                    )
                    isbn_value = str(result.get("isbn") or "").strip()
                    isbn_raw_value = str(result.get("isbn_raw") or "").strip()
                    _queue_form_patch(
                        selected_id,
                        isbn_raw=isbn_raw_value,
                        isbn=isbn_value,
                    )
                    st.session_state["book_review_flash_success"] = (
                        f"ISBN {isbn_value} guardado. El libro ha salido de revisión."
                    )
                    st.rerun()
            except Exception as exc:
                st.error(f"No se pudo guardar y aprobar el ISBN: {exc}")

    if consolidate_isbn:
        payload = {
            # Consolidar ISBN sin sobreescribir manualmente el texto OCR vigente.
            "credits_text": str(book.get("credits_text") or "").strip() or None,
            "isbn_raw": str(st.session_state.get(isbn_raw_key, "")).strip() or None,
            "isbn": str(st.session_state.get(isbn_key, "")).strip() or None,
        }
        try:
            result = api_put(f"/books/{selected_id}/ocr", json=payload, timeout=20.0)
            if isinstance(result, dict):
                isbn_value = str(result.get("isbn") or "").strip()
                isbn_raw_value = str(result.get("isbn_raw") or "").strip()
                _queue_form_patch(selected_id, isbn_raw=isbn_raw_value, isbn=isbn_value)
                with st.expander("Resultado de consolidación ISBN", expanded=False):
                    st.json(result)
            st.session_state["book_review_flash_success"] = (
                "ISBN guardado en la base de datos"
            )
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo consolidar ISBN: {exc}")

st.divider()
st.subheader("Acciones de revisión")

if bool(book.get("workflow_needs_review")):
    review_reason = str(
        book.get("workflow_review_reason") or "Requiere revisión manual"
    )
    st.warning(review_reason)
    origin_stage = _review_origin_stage(book)
    accepts_missing_isbn = (
        review_reason.strip().lower().startswith("ocr_isbn_validation")
    )

    if origin_stage:
        st.caption(
            f"Entró en revisión desde la etapa: **{STAGE_LABELS.get(origin_stage, origin_stage)}**"
        )
    else:
        st.caption(
            "No se pudo inferir con precisión la etapa de origen de la revisión."
        )

    retry_options = _retry_stage_options(book, origin_stage=origin_stage)
    default_retry_stage = (
        origin_stage if origin_stage in retry_options else retry_options[-1]
    )
    retry_selector_key = f"book_review_retry_stage_{selected_id}"
    if retry_selector_key not in st.session_state:
        st.session_state[retry_selector_key] = default_retry_stage

    approve_label = (
        "Aceptar que no tiene ISBN y salir de la revisión"
        if accepts_missing_isbn
        else "Aprobar y salir de la revisión"
    )
    if st.button(approve_label, key="book_review_approve"):
        try:
            api_post(
                f"/workflow/review/{selected_id}",
                json={"action": "approve"},
                timeout=60.0,
            )
            if accepts_missing_isbn:
                st.success(
                    "Ausencia de ISBN aceptada. El libro puede continuar desde los metadatos."
                )
            else:
                st.success("Libro aprobado")
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo aprobar el libro: {exc}")

    with st.expander("Configurar y repetir una fase", expanded=True):
        retry_stage = st.selectbox(
            "Etapa a reintentar",
            retry_options,
            key=retry_selector_key,
            format_func=lambda value: STAGE_LABELS.get(str(value), str(value)),
        )

        retry_stop_after: str | None = None
        ocr_in_retry = False
        metadata_in_retry = False
        catalog_in_retry = False

        if retry_stage == "cover":
            st.caption(
                "La descarga de portada se repetirá con sobrescritura y no altera "
                "las etapas del flujo principal."
            )
        else:
            main_stages = list(STAGE_ORDER[:-1])
            retry_start_idx = main_stages.index(retry_stage)
            stop_options = ["(sin límite)", *main_stages[retry_start_idx:]]
            stop_key = f"book_review_retry_stop_after_{selected_id}"
            previous_stage_key = f"book_review_retry_previous_stage_{selected_id}"
            previous_stage = str(st.session_state.get(previous_stage_key) or "").strip()
            if (
                stop_key not in st.session_state
                or previous_stage != retry_stage
                or st.session_state.get(stop_key) not in stop_options
            ):
                st.session_state[stop_key] = retry_stage
            selected_stop = st.selectbox(
                "Parar en",
                stop_options,
                key=stop_key,
                format_func=lambda value: (
                    value
                    if value == "(sin límite)"
                    else STAGE_LABELS.get(str(value), str(value))
                ),
            )
            st.session_state[previous_stage_key] = retry_stage
            retry_stop_after = (
                None if selected_stop == "(sin límite)" else str(selected_stop)
            )
            retry_stop_idx = (
                len(main_stages) - 1
                if retry_stop_after is None
                else main_stages.index(retry_stop_after)
            )
            ocr_in_retry = retry_start_idx <= 0 <= retry_stop_idx
            metadata_in_retry = retry_start_idx <= 1 <= retry_stop_idx
            catalog_in_retry = retry_start_idx <= 2 <= retry_stop_idx

        max_attempts_key = f"book_review_retry_max_attempts_{selected_id}"
        seed_widget_once(max_attempts_key, 2)
        retry_max_attempts = st.number_input(
            "Reintentos máximos",
            min_value=0,
            max_value=20,
            key=max_attempts_key,
            disabled=retry_stage == "cover",
        )

        ollama_models: list[str] = []
        if ocr_in_retry or catalog_in_retry:
            try:
                ollama_models = load_ollama_models()
            except Exception as exc:
                st.warning(f"No se pudieron cargar modelos de Ollama: {exc}")

        retry_ocr_provider: str | None = None
        retry_ocr_model: str | None = None
        retry_ocr_resize = False
        if ocr_in_retry:
            st.caption("Configuración de OCR")
            ocr_col_a, ocr_col_b = st.columns([1, 2])
            with ocr_col_a:
                ocr_provider_options = ["openai", "ollama"]
                ocr_provider_default = (
                    OCR_PROVIDER_DEFAULT
                    if OCR_PROVIDER_DEFAULT in ocr_provider_options
                    else "ollama"
                )
                ocr_provider_key = f"book_review_retry_ocr_provider_{selected_id}"
                seed_widget_once(ocr_provider_key, ocr_provider_default)
                retry_ocr_provider = st.selectbox(
                    "Proveedor de OCR",
                    ocr_provider_options,
                    key=ocr_provider_key,
                )
            with ocr_col_b:
                if retry_ocr_provider == "ollama":
                    retry_ocr_model = render_ollama_model_selector(
                        label="Modelo OCR",
                        key=f"book_review_retry_ocr_model_ollama_{selected_id}",
                        installed_models=ollama_models,
                        default_model=OCR_OLLAMA_MODEL_DEFAULT,
                        suggested_models=OCR_OLLAMA_MODEL_SUGGESTIONS,
                    )
                else:
                    retry_ocr_model = render_openai_ocr_model_selector(
                        label="Modelo OCR",
                        key=f"book_review_retry_ocr_model_openai_{selected_id}",
                        default_model=OCR_OPENAI_MODEL_DEFAULT,
                    )

                resize_key = f"book_review_retry_ocr_resize_{selected_id}"
                seed_widget_once(resize_key, OCR_RESIZE_TO_1800_DEFAULT)
                retry_ocr_resize = st.checkbox(
                    "Reducir imagen a 1800 px (solo para glm-ocr)",
                    key=resize_key,
                    help=(
                        "Si está activo y el modelo OCR empieza por 'glm-ocr', "
                        "la imagen se reduce a un lado máximo de 1800 px."
                    ),
                )

        retry_download_cover = False
        if metadata_in_retry:
            download_cover_key = (
                f"book_review_retry_download_cover_after_metadata_{selected_id}"
            )
            seed_widget_once(download_cover_key, True)
            retry_download_cover = st.checkbox(
                "Descargar portadas al obtener metadatos",
                key=download_cover_key,
                help=(
                    "Después de consultar las API, descarga la portada si este ID "
                    "todavía no tiene una. Un fallo de portada no detiene el flujo."
                ),
            )

        retry_catalog_provider: str | None = None
        retry_catalog_model: str | None = None
        if catalog_in_retry:
            st.caption("Configuración de catalogación automática")
            catalog_col_a, catalog_col_b = st.columns([1, 2])
            with catalog_col_a:
                catalog_provider_options = ["openai", "ollama"]
                catalog_provider_default = (
                    CATALOG_PROVIDER_DEFAULT
                    if CATALOG_PROVIDER_DEFAULT in catalog_provider_options
                    else "ollama"
                )
                catalog_provider_key = (
                    f"book_review_retry_catalog_provider_{selected_id}"
                )
                seed_widget_once(catalog_provider_key, catalog_provider_default)
                retry_catalog_provider = st.selectbox(
                    "Proveedor del catálogo",
                    catalog_provider_options,
                    key=catalog_provider_key,
                )
            with catalog_col_b:
                if retry_catalog_provider == "ollama":
                    retry_catalog_model = render_ollama_model_selector(
                        label="Modelo del catálogo",
                        key=f"book_review_retry_catalog_model_ollama_{selected_id}",
                        installed_models=ollama_models,
                        default_model=CATALOG_OLLAMA_MODEL_DEFAULT,
                        suggested_models=CATALOG_OLLAMA_MODEL_SUGGESTIONS,
                    )
                else:
                    catalog_model_key = (
                        f"book_review_retry_catalog_model_openai_{selected_id}"
                    )
                    retry_catalog_model = render_openai_catalog_model_selector(
                        label="Modelo del catálogo",
                        key=catalog_model_key,
                        default_model=CATALOG_OPENAI_MODEL_DEFAULT,
                    )

        st.caption(
            "La repetición sobrescribe desde la etapa elegida; la ficha que está "
            "en revisión queda autorizada para ese reintento."
        )
        if st.button(
            "Reintentar con esta configuración",
            key="book_review_retry",
            type="primary",
        ):
            try:
                with st.spinner("Enviando el reintento a Orquestación…"):
                    response = api_post(
                        "/workflow/jobs",
                        json={
                            "book_id": selected_id,
                            "block": scope_block,
                            "module": scope_module,
                            "limit": 1,
                            "start_stage": retry_stage,
                            "action": f"retry_from_{retry_stage}",
                            "stop_after": retry_stop_after,
                            "overwrite": True,
                            "max_attempts": int(retry_max_attempts),
                            "download_cover_after_metadata": bool(retry_download_cover),
                            "ocr_provider": retry_ocr_provider,
                            "ocr_model": (
                                retry_ocr_model.strip()
                                if isinstance(retry_ocr_model, str)
                                and retry_ocr_model.strip()
                                else None
                            ),
                            "ocr_resize_to_1800": bool(retry_ocr_resize),
                            "catalog_provider": retry_catalog_provider,
                            "catalog_model": (
                                retry_catalog_model.strip()
                                if isinstance(retry_catalog_model, str)
                                and retry_catalog_model.strip()
                                else None
                            ),
                        },
                        timeout=15.0,
                    )
                started_job = response.get("job") or {}
                started_job_id = str(started_job.get("job_id") or "").strip()
                if not started_job_id:
                    raise RuntimeError(
                        "El backend no devolvió el identificador del trabajo"
                    )
                st.session_state["orq_last_workflow_job_id"] = started_job_id
                stage_label = STAGE_LABELS.get(str(retry_stage), str(retry_stage))
                st.success(
                    f"Reintento desde {stage_label} enviado a Orquestación. "
                    "Puedes seguir allí su progreso y detenerlo."
                )
                st.page_link(
                    "pages/01_orquestacion.py",
                    label="Abrir Orquestación",
                    icon=":material/account_tree:",
                )
            except Exception as exc:
                st.error(f"No se pudo enviar el reintento a Orquestación: {exc}")
else:
    st.info("Este libro no está en revisión.")

with st.expander("Traza OCR", expanded=False):
    st.json(book.get("ocr_trace") or {})

metadata_payload = (
    book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
)
provider_statuses = metadata_payload.get("provider_statuses")
if isinstance(provider_statuses, dict):
    st.caption("Estado de las fuentes bibliográficas")
    status_labels = {
        "fetched": "Con datos",
        "empty": "Sin resultados",
        "error": "Error",
        "not_fetched": "No consultado",
    }
    provider_columns = st.columns(3)
    for column, (provider_key, provider_label) in zip(
        provider_columns,
        (
            ("google", "Google Books"),
            ("open_library", "Open Library"),
            ("isbndb", "ISBNdb"),
        ),
        strict=True,
    ):
        provider_status = (
            str(provider_statuses.get(provider_key) or "not_fetched").strip().lower()
        )
        column.metric(
            provider_label,
            status_labels.get(provider_status, provider_status or "Desconocido"),
        )

    provider_errors = metadata_payload.get("errors")
    if isinstance(provider_errors, dict):
        for provider_key, provider_error in provider_errors.items():
            error_text = str(provider_error or "").strip()
            if error_text:
                st.caption(f"{provider_key}: {error_text}")

with st.expander("Editar metadatos (JSON)", expanded=False):
    metadata_json = json.dumps(book.get("metadata") or {}, ensure_ascii=False, indent=2)
    metadata_text = st.text_area("metadata_json", value=metadata_json, height=240)
    if st.button("Guardar metadatos", key=f"save_metadata_{selected_id}"):
        try:
            payload = {"metadata": json.loads(metadata_text)}
            api_put(f"/books/{selected_id}/metadata", json=payload, timeout=30.0)
            st.success("Metadatos actualizados")
            st.rerun()
        except json.JSONDecodeError as exc:
            st.error(f"JSON inválido: {exc}")
        except Exception as exc:
            st.error(f"No se pudieron actualizar los metadatos: {exc}")

with st.expander("Editar catálogo (JSON)", expanded=False):
    catalog_json = json.dumps(book.get("catalog") or {}, ensure_ascii=False, indent=2)
    catalog_text = st.text_area("catalog_json", value=catalog_json, height=240)
    if st.button("Guardar catálogo", key=f"save_catalog_{selected_id}"):
        try:
            payload = {"catalog": json.loads(catalog_text)}
            api_put(f"/books/{selected_id}/catalog", json=payload, timeout=30.0)
            st.success("Catálogo actualizado")
            st.rerun()
        except json.JSONDecodeError as exc:
            st.error(f"JSON inválido: {exc}")
        except Exception as exc:
            st.error(f"No se pudo actualizar el catálogo: {exc}")
