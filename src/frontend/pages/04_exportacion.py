from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        BLOCK_OPTIONS,
        api_get,
        api_get_bytes,
        configure_page,
        get_selected_scope,
        list_existing_modules,
        set_selected_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        BLOCK_OPTIONS,
        api_get,
        api_get_bytes,
        configure_page,
        get_selected_scope,
        list_existing_modules,
        set_selected_scope,
        show_backend_status,
    )

configure_page("Exportación | Media Catalog Books")

st.title("Fase 4 · Exportación")
show_backend_status()

st.write("Exporta la vista `libros_carga_abebooks` en TXT tabulado (TAB + cabecera).")

current_block, current_module = get_selected_scope()
block = st.selectbox("Bloque", BLOCK_OPTIONS, index=BLOCK_OPTIONS.index(current_block), key="export_block_selector")
available_modules = list_existing_modules(block)
modules_key = f"export_modules_selected_{block}"

if available_modules:
    if modules_key not in st.session_state:
        if block == current_block and current_module in available_modules:
            st.session_state[modules_key] = [current_module]
        else:
            st.session_state[modules_key] = [available_modules[0]]
    else:
        current_values = [str(item).zfill(2) for item in st.session_state.get(modules_key, [])]
        filtered_values = [item for item in current_values if item in available_modules]
        if filtered_values != current_values:
            st.session_state[modules_key] = filtered_values

    selected_modules = st.multiselect("Modulos", options=available_modules, key=modules_key)
else:
    selected_modules = []
    st.warning(f"No hay módulos para el bloque {block}.")

selected_modules = [str(item).zfill(2) for item in selected_modules if str(item).zfill(2) in available_modules]
set_selected_scope(block, selected_modules[0] if selected_modules else None)

if selected_modules:
    prefixes = [f"{module}{block}" for module in selected_modules]
    st.caption("Selección activa")
    st.markdown(" ".join(f"`{prefix}`" for prefix in prefixes))
else:
    prefixes = []
    st.info("Selecciona al menos un módulo para exportar.")

encoding = st.selectbox(
    "Codificación del fichero",
    ["windows-1252", "utf-8"],
    index=0,
    key="export_encoding_selector",
)

export_disabled = not prefixes
if st.button("Exportar TXT", type="primary", disabled=export_disabled):
    params = {"block": block, "modules": ",".join(selected_modules), "encoding": encoding}
    try:
        result = api_get("/export/books/txt", params=params, timeout=180.0)
        path = Path(str(result.get("path") or "data/output/exports/books.txt"))
        filename = str(result.get("filename") or path.name)
        rows = int(result.get("rows") or 0)
        used_encoding = str(result.get("encoding") or encoding)
        st.success(f"Exportado en servidor: {path} ({rows} filas, {used_encoding})")

        try:
            file_bytes = api_get_bytes("/export/books/file", params={"filename": filename}, timeout=180.0)
            st.session_state["export_last_file_bytes"] = file_bytes
            st.session_state["export_last_file_name"] = filename
            st.session_state["export_last_file_mime"] = "text/plain"
        except Exception as exc:
            st.session_state.pop("export_last_file_bytes", None)
            st.session_state.pop("export_last_file_name", None)
            st.session_state.pop("export_last_file_mime", None)
            st.warning(f"El fichero se guardo en el servidor, pero no se pudo preparar la descarga: {exc}")

        if path.exists():
            st.caption(f"Tamano: {path.stat().st_size} bytes")
    except Exception as exc:
        st.error(f"Error exportando TXT: {exc}")

download_bytes = st.session_state.get("export_last_file_bytes")
download_name = st.session_state.get("export_last_file_name")
download_mime = st.session_state.get("export_last_file_mime", "text/plain")
if isinstance(download_bytes, (bytes, bytearray)) and str(download_name or "").strip():
    st.download_button(
        "Descargar TXT en este equipo",
        data=bytes(download_bytes),
        file_name=str(download_name),
        mime=str(download_mime),
        key="export_download_button",
    )

st.subheader("Preview de exportación")
preview_limit = st.number_input("Filas maximas", min_value=10, max_value=5000, value=300, step=10)

if prefixes:
    try:
        payload = api_get(
            "/export/books/preview",
            params={"block": block, "modules": ",".join(selected_modules), "limit": int(preview_limit)},
            timeout=60.0,
        )
        rows = payload.get("rows", []) if isinstance(payload, dict) else []
        if rows:
            df = pd.DataFrame(rows)
            preferred_cols = [
                "listingid",
                "title",
                "author",
                "publishername",
                "isbn",
                "language",
                "producttype",
                "bindingtext",
                "bookcondition",
                "price",
                "quantity",
                "imgurl",
            ]
            visible_cols = [col for col in preferred_cols if col in df.columns]
            if not visible_cols:
                visible_cols = list(df.columns)
            st.dataframe(df[visible_cols], width="stretch", hide_index=True)
            st.caption(f"Mostrando {len(df)} fila(s)")
        else:
            st.info("No hay datos para la selección actual.")
    except Exception as exc:
        st.error(f"No se pudo cargar la preview: {exc}")
