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
for row in rows:
    book_id = str(row.get("id") or "").strip()
    if not book_id:
        continue
    title = str(row.get("titulo") or "").strip() or "(sin título)"
    author = str(row.get("autor") or "").strip() or "(sin autor)"
    labels[book_id] = f"{book_id} | {title} | {author}"

selector_key = "core_catalog_book_selector"
preferred_id = get_selected_book_id()
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
        st.session_state[selector_key] = ids[current_index - 1]
        st.rerun()
with nav_col_next:
    if st.button("Siguiente →", disabled=current_index >= len(ids) - 1, use_container_width=True):
        st.session_state[selector_key] = ids[current_index + 1]
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
    return str(value if value is not None else "")


def _sync_defaults(book_id: str, payload: dict[str, Any], fields: list[str]) -> None:
    force_reset = st.session_state.get("core_catalog_last_book_id") != book_id
    for field in fields:
        key = _input_key(book_id, field)
        if force_reset or key not in st.session_state:
            st.session_state[key] = _value_or_empty(payload.get(field))
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


def _label_class(field: str) -> str:
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
    if field in {"paginas", "peso", "alto", "ancho", "fondo", "dimensiones"}:
        return "lbl-cyan"
    return "lbl-steel"


def _render_field(label: str, field: str, *, left_col, right_col) -> None:
    current_value = str(st.session_state.get(_input_key(selected_id, field), "")).strip()
    left_col.markdown(
        f"<div class='access-label {_label_class(field)}'>{label}</div>",
        unsafe_allow_html=True,
    )
    key = _input_key(selected_id, field)
    options = _field_options(field, current_value)
    if len(options) > 1:
        index = options.index(current_value) if current_value in options else 0
        right_col.selectbox(
            label,
            options,
            index=index,
            key=key,
            label_visibility="collapsed",
        )
    else:
        right_col.text_input(label, key=key, label_visibility="collapsed")


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
            ("Dimensiones", "dimensiones"),
            ("URL de imágenes", "url_imagenes"),
            ("Plantilla de envío", "plantilla_envio"),
            ("Cantidad", "cantidad"),
            ("Precio", "precio"),
            ("Unidad de peso", "unidad_peso"),
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


def _render_group(group_title: str, fields: list[tuple[str, str]]) -> None:
    st.markdown(f"<div class='access-section'>{group_title}</div>", unsafe_allow_html=True)
    for label, field in fields:
        row_label, row_input = st.columns([0.36, 0.64], gap="small")
        _render_field(label, field, left_col=row_label, right_col=row_input)


left_fields_flat = _flatten_fields(LEFT_GROUPS)
right_fields_flat = _flatten_fields(RIGHT_GROUPS)
all_fields = left_fields_flat + [field for field in right_fields_flat if field not in left_fields_flat]
_sync_defaults(selected_id, book, all_fields)

st.markdown("<div class='access-panel'>", unsafe_allow_html=True)
with st.form(f"core_catalog_form_{selected_id}"):
    header_col_a, header_col_b = st.columns([0.35, 0.65], gap="small")
    with header_col_a:
        st.markdown("<div class='access-label lbl-blue'>Ref. del artículo</div>", unsafe_allow_html=True)
    with header_col_b:
        st.text_input("Ref", value=selected_id, disabled=True, label_visibility="collapsed")

    col_left, col_right = st.columns(2, gap="large")

    with col_left:
        for group_title, fields in LEFT_GROUPS:
            _render_group(group_title, fields)

    with col_right:
        for group_title, fields in RIGHT_GROUPS:
            _render_group(group_title, fields)

    st.markdown("<div class='access-section'>Descripción</div>", unsafe_allow_html=True)
    st.markdown("<div class='desc-box'>", unsafe_allow_html=True)
    st.text_area(
        "Descripción actual",
        value=str(book.get("descripcion") or ""),
        height=220,
        disabled=True,
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)

    save = st.form_submit_button("Guardar cambios y actualizar descripción", type="primary", use_container_width=True)

if save:
    payload_fields: dict[str, Any] = {}
    for field in all_fields:
        payload_fields[field] = st.session_state.get(_input_key(selected_id, field))

    try:
        result = api_put(
            f"/core-books/{selected_id}",
            json={"fields": payload_fields, "recompute_description": True},
            timeout=60.0,
        )
        item = result.get("book") if isinstance(result, dict) else {}
        if isinstance(item, dict):
            for field in all_fields:
                st.session_state[_input_key(selected_id, field)] = _value_or_empty(item.get(field))
        st.success("Registro guardado y descripción actualizada")
        st.rerun()
    except Exception as exc:
        st.error(f"No se pudo guardar el formulario: {exc}")

st.markdown("</div>", unsafe_allow_html=True)
