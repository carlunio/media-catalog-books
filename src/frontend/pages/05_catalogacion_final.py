from pathlib import Path
from typing import Any

import requests
import streamlit as st

try:
    from src.frontend.utils import (
        api_get,
        api_post,
        api_put,
        configure_page,
        get_selected_book_id,
        scope_params,
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
        get_selected_book_id,
        scope_params,
        select_module_scope,
        set_selected_book_id,
        show_backend_status,
    )

configure_page("Catalogación final | Media Catalog Books")
show_backend_status()

ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets"
ACCESS_FORM_CSS_PATH = ASSETS_DIR / "catalog_form_access.css"


def _load_page_css() -> str:
    try:
        return ACCESS_FORM_CSS_PATH.read_text(encoding="utf-8")
    except OSError:
        return ""


page_css = _load_page_css()
if page_css:
    st.markdown(f"<style>{page_css}</style>", unsafe_allow_html=True)

st.markdown("<div class='access-titlebar'>Formulario de catalogación</div>", unsafe_allow_html=True)

scope_block, scope_module = select_module_scope(key_prefix="core_catalog_scope", title="Módulo de trabajo")
if not scope_module:
    st.stop()

scope = scope_params(scope_block, scope_module)

top_col_a, top_col_b = st.columns([1, 1])
with top_col_a:
    st.caption(f"Bloque {scope_block} · Módulo {scope_module}")
with top_col_b:
    if st.button("Sincronizar desde catalogación automática", use_container_width=True):
        try:
            result = api_post("/core-books/bootstrap", params={**scope, "limit": 5000}, timeout=90.0)
            st.success(f"Registros sincronizados: {int(result.get('upserted') or 0)}")
        except Exception as exc:
            st.error(f"No se pudo sincronizar: {exc}")

try:
    options_payload = api_get("/core-books/options", timeout=10.0)
    allowed_values = options_payload.get("allowed_values") if isinstance(options_payload, dict) else {}
    if not isinstance(allowed_values, dict):
        allowed_values = {}
except Exception as exc:
    st.error(f"No se pudieron cargar valores permitidos: {exc}")
    st.stop()

try:
    rows = api_get("/core-books", params={"limit": 5000, **scope}, timeout=20.0)
except Exception as exc:
    st.error(f"No se pudo cargar la tabla books: {exc}")
    st.stop()

if not rows:
    st.info("No hay registros en books para este módulo. Pulsa sincronizar para crearlos desde catalogación.")
    st.stop()

ids = [str(row.get("id") or "").strip() for row in rows if str(row.get("id") or "").strip()]
labels: dict[str, str] = {}


def _display_text(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


for row in rows:
    book_id = str(row.get("id") or "").strip()
    if not book_id:
        continue
    title = _display_text(row.get("titulo")) or "(sin título)"
    author = _display_text(row.get("autor")) or "(sin autor)"
    labels[book_id] = f"{book_id} | {title} | {author}"

selector_key = "core_catalog_book_selector"
selector_pending_key = "core_catalog_book_selector_pending"
preferred_id = get_selected_book_id()

pending_selected = st.session_state.pop(selector_pending_key, None)
if pending_selected in ids:
    st.session_state[selector_key] = pending_selected

if selector_key not in st.session_state:
    if preferred_id in ids:
        st.session_state[selector_key] = preferred_id
    else:
        st.session_state[selector_key] = ids[0]
elif st.session_state.get(selector_key) not in ids:
    st.session_state[selector_key] = ids[0]

selected_id = st.selectbox(
    "Ref. del artículo",
    ids,
    key=selector_key,
    format_func=lambda value: labels.get(value, value),
)
set_selected_book_id(selected_id)

current_index = ids.index(selected_id)
nav_col_prev, nav_col_next, _ = st.columns([1, 1, 4])
with nav_col_prev:
    if st.button("← Anterior", disabled=current_index == 0, use_container_width=True):
        st.session_state[selector_pending_key] = ids[current_index - 1]
        st.rerun()
with nav_col_next:
    if st.button("Siguiente →", disabled=current_index >= len(ids) - 1, use_container_width=True):
        st.session_state[selector_pending_key] = ids[current_index + 1]
        st.rerun()

try:
    book = api_get(f"/core-books/{selected_id}", params={"bootstrap": "true"}, timeout=20.0)
except requests.exceptions.HTTPError as exc:
    st.error(f"No se pudo abrir el registro {selected_id}: {exc}")
    st.stop()
except Exception as exc:
    st.error(f"No se pudo abrir el registro {selected_id}: {exc}")
    st.stop()


def _input_key(book_id: str, field: str) -> str:
    return f"core_catalog_{book_id}_{field}"


def _value_or_empty(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"none", "null", "nan"}:
        return ""
    return text


def _normalize_session_value(field: str, value: Any) -> Any:
    if field == "precio":
        text = _value_or_empty(value).replace(",", ".").replace("€", "").strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0
    if field == "cantidad":
        text = _value_or_empty(value)
        if not text:
            return 1
        try:
            return int(float(text))
        except ValueError:
            return 1
    return _value_or_empty(value)


def _sync_defaults(book_id: str, payload: dict[str, Any], fields: list[str]) -> None:
    force_reset = bool(st.session_state.pop("core_catalog_force_reload", False))
    if st.session_state.get("core_catalog_last_book_id") != book_id:
        force_reset = True
    for field in fields:
        key = _input_key(book_id, field)
        if force_reset or key not in st.session_state:
            st.session_state[key] = _normalize_session_value(field, payload.get(field))
    st.session_state["core_catalog_last_book_id"] = book_id


def _field_options(field: str, current: str) -> list[str]:
    if field.startswith("catalogo_"):
        base = [str(item).strip() for item in allowed_values.get("catalogo", []) if str(item).strip()]
    else:
        base = [str(item).strip() for item in allowed_values.get(field, []) if str(item).strip()]
    options = [""]
    for item in base:
        if item not in options:
            options.append(item)
    if current and current not in options:
        options.append(current)
    return options


READ_ONLY_FIELDS: set[str] = {
    "titulo_corto",
    "subtitulo",
    "titulo_completo",
}

SELECTABLE_FIELDS: set[str] = {
    "tipo_articulo",
    "estado_stock",
    "estado_carga",
    "edicion",
    "numero_impresion",
    "ilustraciones",
    "categoria",
    "genero",
    "encuadernacion",
    "estado_conservacion",
    "estado_cubierta",
    "dedicatorias",
    "plantilla_envio",
    "catalogo_1",
    "catalogo_2",
    "catalogo_3",
}

SELECTABLE_WITH_CUSTOM_VALUE_FIELDS: set[str] = {
    "categoria",
    "genero",
}

SALMON_FIELDS: set[str] = {
    "edicion",
    "numero_impresion",
    "coleccion",
    "numero_coleccion",
    "obra_completa",
    "volumen",
}

IMPORTANT_FIELDS: set[str] = {
    "edicion",
    "numero_coleccion",
    "obra_completa",
    "volumen",
    "detalle_encuadernacion",
    "desperfectos",
    "url_imagenes",
    "plantilla_envio",
    "precio",
}


def _label_class(field: str) -> str:
    if field in READ_ONLY_FIELDS:
        return "lbl-orange-soft"
    if field in SALMON_FIELDS:
        return "lbl-salmon"
    if field in {"tipo_articulo", "categoria", "genero", "palabras_clave"}:
        return "lbl-blue"
    if field in {"estado_stock", "estado_carga", "plantilla_envio", "catalogo_1", "catalogo_2", "catalogo_3", "cantidad", "precio", "url_imagenes"}:
        return "lbl-purple"
    if field in {"titulo", "subtitulo"}:
        return "lbl-orange"
    if field in {"titulo_corto", "titulo_completo", "obra_completa", "volumen", "coleccion", "numero_coleccion"}:
        return "lbl-beige"
    if field in {"autor", "pais_autor", "editorial", "pais_publicacion", "anio", "isbn", "idioma"}:
        return "lbl-green"
    if field in {"encuadernacion", "detalle_encuadernacion", "estado_conservacion", "estado_cubierta", "desperfectos", "dedicatorias"}:
        return "lbl-yellow"
    if field in {"paginas", "peso", "alto", "ancho", "fondo"}:
        return "lbl-cyan"
    return "lbl-steel"


def _render_field(label: str, field: str, *, left_col, right_col) -> None:
    current_value = str(st.session_state.get(_input_key(selected_id, field), "")).strip()
    important_class = " is-important" if field in IMPORTANT_FIELDS else ""
    left_col.markdown(
        f"<div class='access-label {_label_class(field)}{important_class}'>{label}</div>",
        unsafe_allow_html=True,
    )
    key = _input_key(selected_id, field)

    if field in READ_ONLY_FIELDS:
        right_col.text_input(
            label,
            value=current_value,
            disabled=True,
            label_visibility="collapsed",
        )
        return

    options = _field_options(field, current_value)
    if field in SELECTABLE_FIELDS and len(options) > 1:
        select_kwargs: dict[str, Any] = {}
        if field in SELECTABLE_WITH_CUSTOM_VALUE_FIELDS:
            select_kwargs["accept_new_options"] = True
        select_kwargs["on_change"] = _autosave_field
        select_kwargs["args"] = (field,)
        right_col.selectbox(
            label,
            options,
            key=key,
            label_visibility="collapsed",
            **select_kwargs,
        )
    elif field == "precio":
        right_col.number_input(
            label,
            key=key,
            min_value=0.0,
            step=0.5,
            format="%.2f",
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )
    elif field == "cantidad":
        right_col.number_input(
            label,
            key=key,
            min_value=0,
            step=1,
            format="%d",
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )
    else:
        right_col.text_input(
            label,
            key=key,
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )


def _render_inline_field(container: Any, label: str, field: str, *, ratio: tuple[float, float] = (0.36, 0.64)) -> None:
    row_label, row_input = container.columns([ratio[0], ratio[1]], gap="small")
    _render_field(label, field, left_col=row_label, right_col=row_input)


def _render_stacked_field(
    container: Any,
    label: str,
    field: str,
    *,
    label_class: str | None = None,
) -> None:
    important_class = " is-important" if field in IMPORTANT_FIELDS else ""
    resolved_class = label_class or _label_class(field)
    container.markdown(
        f"<div class='access-label {resolved_class}{important_class}'>{label}</div>",
        unsafe_allow_html=True,
    )

    current_value = str(st.session_state.get(_input_key(selected_id, field), "")).strip()
    key = _input_key(selected_id, field)

    if field in READ_ONLY_FIELDS:
        container.text_input(
            label,
            value=current_value,
            disabled=True,
            label_visibility="collapsed",
        )
        return

    options = _field_options(field, current_value)
    if field in SELECTABLE_FIELDS and len(options) > 1:
        select_kwargs: dict[str, Any] = {}
        if field in SELECTABLE_WITH_CUSTOM_VALUE_FIELDS:
            select_kwargs["accept_new_options"] = True
        select_kwargs["on_change"] = _autosave_field
        select_kwargs["args"] = (field,)
        container.selectbox(
            label,
            options,
            key=key,
            label_visibility="collapsed",
            **select_kwargs,
        )
    elif field == "precio":
        container.number_input(
            label,
            key=key,
            min_value=0.0,
            step=0.5,
            format="%.2f",
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )
    elif field == "cantidad":
        container.number_input(
            label,
            key=key,
            min_value=0,
            step=1,
            format="%d",
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )
    else:
        container.text_input(
            label,
            key=key,
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(field,),
        )


def _render_static_stacked_field(
    container: Any,
    *,
    label: str,
    value: str,
    label_class: str = "lbl-blue",
    key_suffix: str,
) -> None:
    container.markdown(
        f"<div class='access-label {label_class}'>{label}</div>",
        unsafe_allow_html=True,
    )
    container.text_input(
        label,
        value=value,
        disabled=True,
        key=f"core_catalog_static_{selected_id}_{key_suffix}",
        label_visibility="collapsed",
    )


LEFT_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "Ficha bibliográfica",
        [
            ("Tipo de artículo", "tipo_articulo"),
            ("Título", "titulo"),
            ("Título corto", "titulo_corto"),
            ("Subtítulo", "subtitulo"),
            ("Título completo", "titulo_completo"),
            ("Autor", "autor"),
            ("País del autor", "pais_autor"),
            ("Editorial", "editorial"),
            ("País de la publicación", "pais_publicacion"),
            ("Año de publicación", "anio"),
            ("ISBN", "isbn"),
            ("Idioma", "idioma"),
        ],
    ),
    (
        "Edición y colección",
        [
            ("Edición", "edicion"),
            ("Número de impresión", "numero_impresion"),
            ("Colección", "coleccion"),
            ("Nº en la colección", "numero_coleccion"),
            ("Título de la obra completa", "obra_completa"),
            ("Volumen", "volumen"),
        ],
    ),
    (
        "Contribuciones",
        [
            ("Traductor", "traductor"),
            ("Ilustrador", "ilustrador"),
            ("Editor", "editor"),
            ("Fotografía de", "fotografia_de"),
            ("Introducción de", "introduccion_de"),
            ("Epílogo de", "epilogo_de"),
            ("Info. sobre ilustraciones", "ilustraciones"),
        ],
    ),
]

RIGHT_GROUPS: list[tuple[str, list[tuple[str, str]]]] = [
    (
        "Clasificación y estado",
        [
            ("Estado de stock", "estado_stock"),
            ("Estado de carga", "estado_carga"),
            ("Categoría", "categoria"),
            ("Género", "genero"),
            ("Palabras clave", "palabras_clave"),
            ("Encuadernación", "encuadernacion"),
            ("Detalles de la encuadernación", "detalle_encuadernacion"),
            ("Estado de conservación", "estado_conservacion"),
            ("Estado de la cubierta", "estado_cubierta"),
            ("Desperfectos", "desperfectos"),
            ("Dedicatorias", "dedicatorias"),
        ],
    ),
    (
        "Medidas y venta",
        [
            ("Nº de páginas", "paginas"),
            ("Alto", "alto"),
            ("Ancho", "ancho"),
            ("Fondo", "fondo"),
            ("Peso", "peso"),
            ("URL de imágenes", "url_imagenes"),
            ("Plantilla de envío", "plantilla_envio"),
            ("Cantidad", "cantidad"),
            ("Precio", "precio"),
            ("Catálogo 1", "catalogo_1"),
            ("Catálogo 2", "catalogo_2"),
            ("Catálogo 3", "catalogo_3"),
        ],
    ),
]


def _flatten_fields(groups: list[tuple[str, list[tuple[str, str]]]]) -> list[str]:
    flattened: list[str] = []
    for _, rows in groups:
        for _, field in rows:
            if field not in flattened:
                flattened.append(field)
    return flattened


left_fields_flat = _flatten_fields(LEFT_GROUPS)
right_fields_flat = _flatten_fields(RIGHT_GROUPS)
all_fields = left_fields_flat + [field for field in right_fields_flat if field not in left_fields_flat]
description_field = "descripcion"
sync_fields = all_fields + [description_field]
_sync_defaults(selected_id, book, sync_fields)
description_key = _input_key(selected_id, description_field)

st.session_state["core_catalog_current_book_id"] = selected_id
st.session_state["core_catalog_all_fields"] = list(all_fields)
st.session_state["core_catalog_description_field"] = description_field


def _payload_from_session(book_id: str) -> dict[str, Any]:
    payload_fields: dict[str, Any] = {}
    for field in all_fields:
        payload_fields[field] = st.session_state.get(_input_key(book_id, field))
    payload_fields[description_field] = st.session_state.get(_input_key(book_id, description_field))
    return payload_fields


def _single_field_payload_from_session(book_id: str, field: str) -> dict[str, Any]:
    return {field: st.session_state.get(_input_key(book_id, field))}


def _apply_saved_book_to_session(book_id: str, payload: dict[str, Any]) -> None:
    for field in sync_fields:
        st.session_state[_input_key(book_id, field)] = _normalize_session_value(field, payload.get(field))


def _save_current_book(
    book_id: str,
    *,
    recompute_description: bool,
    timeout: float = 60.0,
    fields_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload_fields = fields_override if isinstance(fields_override, dict) else _payload_from_session(book_id)
    result = api_put(
        f"/core-books/{book_id}",
        json={
            "fields": payload_fields,
            "recompute_description": bool(recompute_description),
        },
        timeout=timeout,
    )
    payload = result.get("book") if isinstance(result, dict) else None
    if isinstance(payload, dict) and not bool(st.session_state.get("core_catalog_manual_save_in_progress")):
        _apply_saved_book_to_session(book_id, payload)
    return result if isinstance(result, dict) else {}


def _autosave_field(field: str) -> None:
    book_id = str(st.session_state.get("core_catalog_current_book_id") or "").strip()
    if not book_id:
        return
    try:
        should_recompute_description = field != description_field
        fields_override = _single_field_payload_from_session(book_id, field)
        _save_current_book(
            book_id,
            recompute_description=should_recompute_description,
            timeout=45.0,
            fields_override=fields_override,
        )
        st.session_state["core_catalog_autosave_error"] = ""
        st.session_state["core_catalog_autosave_last_field"] = field
    except Exception as exc:
        st.session_state["core_catalog_autosave_error"] = f"No se pudo autoguardar '{field}': {exc}"


autosave_error = str(st.session_state.get("core_catalog_autosave_error") or "").strip()
if autosave_error:
    st.error(autosave_error)
flash_success = str(st.session_state.pop("core_catalog_flash_success", "") or "").strip()
if flash_success:
    st.success(flash_success)

st.markdown("<div class='access-panel'>", unsafe_allow_html=True)
save = False
create_description = False
col_left, col_right = st.columns(2, gap="large")

with col_left:
    # Top strip: 1/3 ref, 1/3 tipo, 1/3 estado stock/carga.
    top_ref, top_tipo, top_estado = st.columns([1, 1, 1], gap="small")
    _render_static_stacked_field(
        top_ref,
        label="Ref. del artículo",
        value=selected_id,
        label_class="lbl-blue",
        key_suffix="ref",
    )
    _render_stacked_field(top_tipo, "Tipo de artículo", "tipo_articulo")
    _render_inline_field(top_estado, "Estado de stock", "estado_stock", ratio=(0.52, 0.48))
    _render_inline_field(top_estado, "Estado de carga", "estado_carga", ratio=(0.52, 0.48))

    _render_inline_field(st, "Título", "titulo", ratio=(0.34, 0.66))

    titulo_corto_col, subtitulo_col = st.columns(2, gap="small")
    _render_inline_field(titulo_corto_col, "Título corto", "titulo_corto", ratio=(0.34, 0.66))
    _render_inline_field(subtitulo_col, "Subtítulo", "subtitulo", ratio=(0.34, 0.66))

    _render_inline_field(st, "Título completo", "titulo_completo", ratio=(0.32, 0.68))

    autor_col, pais_autor_col = st.columns([0.64, 0.36], gap="small")
    _render_inline_field(autor_col, "Autor", "autor", ratio=(0.3, 0.7))
    _render_inline_field(pais_autor_col, "País del autor", "pais_autor", ratio=(0.45, 0.55))

    editorial_col, isbn_col = st.columns([0.68, 0.32], gap="small")
    _render_inline_field(editorial_col, "Editorial", "editorial", ratio=(0.3, 0.7))
    _render_inline_field(isbn_col, "ISBN", "isbn", ratio=(0.42, 0.58))

    pais_pub_col, idioma_col, anio_col = st.columns([0.48, 0.35, 0.17], gap="small")
    _render_inline_field(pais_pub_col, "País de la publicación", "pais_publicacion", ratio=(0.58, 0.42))
    _render_inline_field(idioma_col, "Idioma", "idioma", ratio=(0.38, 0.62))
    _render_inline_field(anio_col, "Año", "anio", ratio=(0.42, 0.58))

    edicion_col, impresion_col = st.columns(2, gap="small")
    _render_inline_field(edicion_col, "Edición", "edicion", ratio=(0.32, 0.68))
    _render_inline_field(impresion_col, "Nº de impresión", "numero_impresion", ratio=(0.34, 0.66))

    coleccion_col, numero_col = st.columns([0.66, 0.34], gap="small")
    _render_inline_field(coleccion_col, "Colección", "coleccion", ratio=(0.47, 0.53))
    _render_inline_field(numero_col, "Nº en la colección", "numero_coleccion", ratio=(0.5, 0.5))

    obra_col, volumen_col = st.columns([0.66, 0.34], gap="small")
    _render_inline_field(obra_col, "Título de la obra completa", "obra_completa", ratio=(0.47, 0.53))
    _render_inline_field(volumen_col, "Volumen", "volumen", ratio=(0.5, 0.5))

    _render_inline_field(st, "Traductor", "traductor", ratio=(0.32, 0.68))
    _render_inline_field(st, "Ilustrador", "ilustrador", ratio=(0.32, 0.68))
    _render_inline_field(st, "Editor", "editor", ratio=(0.32, 0.68))
    _render_inline_field(st, "Fotografía de", "fotografia_de", ratio=(0.32, 0.68))
    _render_inline_field(st, "Introducción de", "introduccion_de", ratio=(0.32, 0.68))
    _render_inline_field(st, "Epílogo de", "epilogo_de", ratio=(0.32, 0.68))
    _render_inline_field(st, "Info. sobre ilustraciones", "ilustraciones", ratio=(0.32, 0.68))

with col_right:
    _render_inline_field(st, "Categoría", "categoria", ratio=(0.33, 0.67))
    _render_inline_field(st, "Género", "genero", ratio=(0.33, 0.67))
    _render_inline_field(st, "Palabras clave", "palabras_clave", ratio=(0.33, 0.67))
    _render_inline_field(st, "Encuadernación", "encuadernacion", ratio=(0.33, 0.67))
    _render_inline_field(st, "Detalles de la encuadernación", "detalle_encuadernacion", ratio=(0.33, 0.67))
    estado_cons_col, estado_cub_col = st.columns(2, gap="small")
    _render_inline_field(estado_cons_col, "Estado de conservación", "estado_conservacion", ratio=(0.5, 0.5))
    _render_inline_field(estado_cub_col, "Estado de la cubierta", "estado_cubierta", ratio=(0.5, 0.5))
    _render_inline_field(st, "Desperfectos", "desperfectos", ratio=(0.33, 0.67))
    _render_inline_field(st, "Dedicatorias", "dedicatorias", ratio=(0.33, 0.67))

    paginas_col, alto_col = st.columns([0.67, 0.33], gap="small")
    _render_inline_field(paginas_col, "Nº de páginas", "paginas", ratio=(0.58, 0.42))
    _render_inline_field(alto_col, "Alto", "alto", ratio=(0.56, 0.44))

    peso_col, ancho_col = st.columns([0.67, 0.33], gap="small")
    _render_inline_field(peso_col, "Peso", "peso", ratio=(0.58, 0.42))
    _render_inline_field(ancho_col, "Ancho", "ancho", ratio=(0.56, 0.44))

    fondo_pad_col, fondo_col = st.columns([0.67, 0.33], gap="small")
    with fondo_pad_col:
        st.markdown("&nbsp;", unsafe_allow_html=True)
    _render_inline_field(fondo_col, "Fondo", "fondo", ratio=(0.56, 0.44))

    url_col, cat1_col = st.columns([0.68, 0.32], gap="small")
    _render_inline_field(url_col, "URL de imágenes", "url_imagenes", ratio=(0.55, 0.45))
    _render_inline_field(cat1_col, "Catálogo 1", "catalogo_1", ratio=(0.56, 0.44))

    envio_col, cat2_col = st.columns([0.68, 0.32], gap="small")
    _render_inline_field(envio_col, "Plantilla de envío", "plantilla_envio", ratio=(0.55, 0.45))
    _render_inline_field(cat2_col, "Catálogo 2", "catalogo_2", ratio=(0.56, 0.44))

    qty_price_col, cat3_col = st.columns([0.68, 0.32], gap="small")
    qty_col, price_col = qty_price_col.columns([0.5, 0.5], gap="small")
    _render_inline_field(qty_col, "Cantidad", "cantidad", ratio=(0.54, 0.46))
    _render_inline_field(price_col, "Precio", "precio", ratio=(0.45, 0.55))
    _render_inline_field(cat3_col, "Catálogo 3", "catalogo_3", ratio=(0.56, 0.44))

    desc_label_col, desc_value_col = st.columns([0.28, 0.72], gap="small")
    with desc_label_col:
        st.markdown("<div class='access-label lbl-green'>Descripción</div>", unsafe_allow_html=True)
        create_description = st.button(
            "Crear descripción automática",
            key=f"core_catalog_create_description_{selected_id}",
            use_container_width=True,
        )
        save = st.button(
            "Guardar cambios",
            key=f"core_catalog_save_{selected_id}",
            type="primary",
            use_container_width=True,
        )
    with desc_value_col:
        st.text_area(
            "Descripción",
            key=description_key,
            height=220,
            label_visibility="collapsed",
            on_change=_autosave_field,
            args=(description_field,),
        )

if save or create_description:
    should_recompute_description = bool(create_description)
    try:
        st.session_state["core_catalog_manual_save_in_progress"] = True
        _save_current_book(selected_id, recompute_description=should_recompute_description, timeout=60.0)
        st.session_state["core_catalog_manual_save_in_progress"] = False
        st.session_state["core_catalog_autosave_error"] = ""
        st.session_state["core_catalog_force_reload"] = True
        if should_recompute_description:
            st.session_state["core_catalog_flash_success"] = "Registro guardado y descripción creada"
        else:
            st.session_state["core_catalog_flash_success"] = "Registro guardado"
        st.rerun()
    except Exception as exc:
        st.session_state["core_catalog_manual_save_in_progress"] = False
        st.error(f"No se pudo guardar el formulario: {exc}")

st.markdown("</div>", unsafe_allow_html=True)
