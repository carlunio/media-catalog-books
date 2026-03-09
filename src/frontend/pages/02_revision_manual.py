import json
import re
from pathlib import Path

import requests
import streamlit as st
from PIL import Image, ImageOps

try:
    from src.frontend.utils import (
        api_get,
        api_post,
        api_put,
        configure_page,
        scope_params,
        select_book_id,
        select_module_scope,
        set_selected_book_id,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        api_get,
        api_post,
        api_put,
        configure_page,
        scope_params,
        select_book_id,
        select_module_scope,
        set_selected_book_id,
        show_backend_status,
    )

ISBN_CANDIDATE_PATTERN = re.compile(r"[0-9XxIiLlOo\- ]{9,}")

configure_page("Revision OCR | Media Catalog Books")
st.title("Fase 2 · Revision OCR e ISBN")
show_backend_status()


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
        "valid_isbn10_checksum": "ISBN-10 valido (checksum)",
        "valid_isbn13_checksum": "ISBN-13 valido (checksum)",
        "invalid_isbn10_checksum": "ISBN-10 invalido (checksum)",
        "invalid_isbn13_checksum": "ISBN-13 invalido (checksum)",
        "invalid_length_or_chars": "Formato invalido (longitud/caracteres)",
        "invalid_format": "Formato invalido",
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
    raw_candidates = [_clean_isbn(match) for match in ISBN_CANDIDATE_PATTERN.findall(body)]
    raw_candidates = _unique([value for value in raw_candidates if value])

    normalized_candidates = _unique([_normalize_ocular_isbn_confusions(value) for value in raw_candidates])
    valid_candidates = _unique([value for value in raw_candidates if _is_valid_isbn(value)])
    normalized_valid_candidates = _unique([value for value in normalized_candidates if _is_valid_isbn(value)])
    candidate_details = [_isbn_candidate_detail(value) for value in raw_candidates]
    normalized_candidate_details = [_isbn_candidate_detail(value) for value in normalized_candidates]

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


def _sync_form_defaults(book_id: str, book: dict) -> tuple[str, str, str]:
    credits_key = f"ocr_review_credits_{book_id}"
    isbn_raw_key = f"ocr_review_isbn_raw_{book_id}"
    isbn_key = f"ocr_review_isbn_{book_id}"

    if credits_key not in st.session_state:
        st.session_state[credits_key] = str(book.get("credits_text") or "")
    if isbn_raw_key not in st.session_state:
        st.session_state[isbn_raw_key] = str(book.get("isbn_raw") or "")
    if isbn_key not in st.session_state:
        st.session_state[isbn_key] = str(book.get("isbn") or "")

    return credits_key, isbn_raw_key, isbn_key


scope_block, scope_module = select_module_scope(key_prefix="review_scope", title="Modulo de trabajo")
if not scope_module:
    st.stop()

mode_labels = {
    "all": "Mostrar todos",
    "review": "Solo en review",
    "review_ocr": "Solo review OCR/ISBN",
}
if "book_review_filter_mode" not in st.session_state:
    st.session_state["book_review_filter_mode"] = "all"

filter_mode = st.segmented_control(
    "Filtro",
    options=list(mode_labels.keys()),
    default=st.session_state["book_review_filter_mode"],
    format_func=lambda value: mode_labels.get(str(value), str(value)),
    key="book_review_filter_mode",
    width="stretch",
)
if filter_mode is None:
    filter_mode = "all"

limit = st.number_input("Limite", min_value=1, max_value=5000, value=1200)

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
    st.info("No hay libros para el modulo seleccionado.")
    st.stop()

filtered_rows = _filter_rows(rows, str(filter_mode))
st.caption(
    f"Filtro actual: {mode_labels.get(str(filter_mode), 'Mostrar todos')} | "
    f"{len(filtered_rows)} de {len(rows)} libros"
)

if not filtered_rows:
    st.info("No hay libros con los filtros actuales.")
    st.stop()

selected_id = select_book_id(filtered_rows, label="Selecciona libro", key="book_ocr_review_selector")
book_ids = [str(row.get("id") or "") for row in filtered_rows if str(row.get("id") or "")]
current_index = book_ids.index(selected_id)

col_prev, col_next = st.columns(2)
with col_prev:
    if st.button("Anterior", disabled=current_index == 0, key="book_ocr_review_prev"):
        set_selected_book_id(book_ids[current_index - 1])
        st.rerun()
with col_next:
    if st.button("Siguiente", disabled=current_index == len(book_ids) - 1, key="book_ocr_review_next"):
        set_selected_book_id(book_ids[current_index + 1])
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
        st.warning("No hay imagen local accesible")

    st.markdown("### Estado")
    st.json(
        {
            "id": book.get("id"),
            "block": book.get("block"),
            "module": book.get("module"),
            "pipeline_stage": book.get("pipeline_stage"),
            "workflow_status": book.get("workflow_status"),
            "workflow_review_reason": book.get("workflow_review_reason"),
            "ocr_status": book.get("ocr_status"),
            "ocr_provider": book.get("ocr_provider"),
            "ocr_model": book.get("ocr_model"),
        }
    )

with right:
    st.markdown("### OCR + ISBN")

    metrics_a, metrics_b, metrics_c = st.columns(3)
    metrics_a.metric("ISBN validado", "si" if bool(book.get("isbn")) else "no")
    metrics_b.metric("Cand. detectados", len(validation_from_text.get("raw_candidates") or []))
    metrics_c.metric("Cand. validos", len(validation_from_text.get("valid_candidates") or []))

    if validation_from_text.get("isbn"):
        st.success(
            f"ISBN sugerido por reglas: {validation_from_text.get('isbn')} "
            f"({validation_from_text.get('source')})"
        )
    elif st.session_state.get(credits_key, "").strip():
        st.warning("No hay ISBN valido detectado en el texto OCR con las reglas actuales.")

    manual_isbn_input = str(st.session_state.get(isbn_key, "")).strip()
    manual_raw_input = str(st.session_state.get(isbn_raw_key, "")).strip()
    manual_candidate = manual_isbn_input or manual_raw_input
    if manual_candidate:
        manual_detail = _isbn_candidate_detail(manual_candidate)
        if bool(manual_detail.get("valid")):
            st.info(f"ISBN manual actual valido: {manual_detail.get('cleaned')}")
        else:
            st.warning(
                "ISBN manual actual invalido: "
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
                "valido": "si" if bool(item.get("valid")) else "no",
                "motivo": _reason_label(item.get("reason")),
            }
            for item in candidate_details
        ]
        st.markdown("Candidatos ISBN detectados")
        st.dataframe(table_rows, hide_index=True, use_container_width=True)

    with st.expander("Detalle de validacion ISBN", expanded=False):
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
            save = st.form_submit_button("Guardar OCR revisado", type="primary")
        with form_col_c:
            consolidate_isbn = st.form_submit_button("Consolidar ISBN en BBDD")

    if suggest:
        suggested = _derive_isbn_from_text(st.session_state.get(credits_key, ""))
        st.session_state[isbn_raw_key] = str(suggested.get("isbn_raw") or "")
        st.session_state[isbn_key] = str(suggested.get("isbn") or "")
        st.rerun()

    if save:
        payload = {
            "credits_text": str(st.session_state.get(credits_key, "")).strip() or None,
            "isbn_raw": str(st.session_state.get(isbn_raw_key, "")).strip() or None,
            "isbn": str(st.session_state.get(isbn_key, "")).strip() or None,
        }
        try:
            result = api_put(f"/books/{selected_id}/ocr", json=payload, timeout=20.0)
            st.success("OCR actualizado")
            if isinstance(result, dict):
                isbn_value = str(result.get("isbn") or "").strip()
                isbn_raw_value = str(result.get("isbn_raw") or "").strip()
                st.session_state[isbn_key] = isbn_value
                st.session_state[isbn_raw_key] = isbn_raw_value
                if isbn_value:
                    st.success(f"ISBN final validado: {isbn_value}")
                else:
                    st.warning("No se pudo validar ISBN final (se guardo OCR y ISBN raw).")
                with st.expander("Resultado de guardado", expanded=False):
                    st.json(result)
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo actualizar OCR: {exc}")

    if consolidate_isbn:
        payload = {
            # Consolidar ISBN sin sobreescribir manualmente el texto OCR vigente.
            "credits_text": str(book.get("credits_text") or "").strip() or None,
            "isbn_raw": str(st.session_state.get(isbn_raw_key, "")).strip() or None,
            "isbn": str(st.session_state.get(isbn_key, "")).strip() or None,
        }
        try:
            result = api_put(f"/books/{selected_id}/ocr", json=payload, timeout=20.0)
            st.success("ISBN consolidado en BBDD")
            if isinstance(result, dict):
                isbn_value = str(result.get("isbn") or "").strip()
                isbn_raw_value = str(result.get("isbn_raw") or "").strip()
                st.session_state[isbn_key] = isbn_value
                st.session_state[isbn_raw_key] = isbn_raw_value
                with st.expander("Resultado de consolidacion ISBN", expanded=False):
                    st.json(result)
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo consolidar ISBN: {exc}")

st.divider()
st.subheader("Acciones de review")

if bool(book.get("workflow_needs_review")):
    st.warning(str(book.get("workflow_review_reason") or "Requiere revision manual"))

    action_col_a, action_col_b = st.columns(2)
    with action_col_a:
        if st.button("Aprobar y salir de review", key="book_review_approve"):
            try:
                api_post(
                    f"/workflow/review/{selected_id}",
                    json={"action": "approve"},
                    timeout=60.0,
                )
                st.success("Libro aprobado")
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudo aprobar el libro: {exc}")

    with action_col_b:
        if st.button("Reintentar OCR", key="book_review_retry_ocr"):
            try:
                api_post(
                    f"/workflow/review/{selected_id}",
                    json={"action": "retry_from_ocr"},
                    timeout=600.0,
                )
                st.success("OCR relanzado desde workflow")
                st.rerun()
            except requests.exceptions.ReadTimeout:
                st.error("Timeout relanzando OCR")
            except Exception as exc:
                st.error(f"No se pudo relanzar OCR: {exc}")
else:
    st.info("Este libro no esta en estado review.")

with st.expander("Traza OCR", expanded=False):
    st.json(book.get("ocr_trace") or {})

with st.expander("Editar metadata (JSON)", expanded=False):
    metadata_json = json.dumps(book.get("metadata") or {}, ensure_ascii=False, indent=2)
    metadata_text = st.text_area("metadata_json", value=metadata_json, height=240)
    if st.button("Guardar metadata", key=f"save_metadata_{selected_id}"):
        try:
            payload = {"metadata": json.loads(metadata_text)}
            api_put(f"/books/{selected_id}/metadata", json=payload, timeout=30.0)
            st.success("Metadata actualizada")
            st.rerun()
        except json.JSONDecodeError as exc:
            st.error(f"JSON invalido: {exc}")
        except Exception as exc:
            st.error(f"No se pudo actualizar metadata: {exc}")

with st.expander("Editar catalogo (JSON)", expanded=False):
    catalog_json = json.dumps(book.get("catalog") or {}, ensure_ascii=False, indent=2)
    catalog_text = st.text_area("catalog_json", value=catalog_json, height=240)
    if st.button("Guardar catalogo", key=f"save_catalog_{selected_id}"):
        try:
            payload = {"catalog": json.loads(catalog_text)}
            api_put(f"/books/{selected_id}/catalog", json=payload, timeout=30.0)
            st.success("Catalogo actualizado")
            st.rerun()
        except json.JSONDecodeError as exc:
            st.error(f"JSON invalido: {exc}")
        except Exception as exc:
            st.error(f"No se pudo actualizar catalogo: {exc}")
