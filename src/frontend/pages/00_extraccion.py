import os

import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        api_get,
        api_post,
        configure_page,
        load_stats,
        scope_params,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        api_get,
        api_post,
        configure_page,
        load_stats,
        scope_params,
        select_module_scope,
        show_backend_status,
    )

configure_page("Extracción | Media Catalog Books")

st.title("Fase 0 · Extracción")
st.caption("Extrae e indexa imágenes de créditos en DuckDB")
show_backend_status()

scope_block, scope_module = select_module_scope(key_prefix="ingesta_scope", title="Módulo de trabajo")
if not scope_module:
    st.stop()

stats = load_stats(block=scope_block, module=scope_module)
col_a, col_b, col_c = st.columns(3)
col_a.metric("Total", stats.get("total", 0))
col_b.metric("Pend. OCR", stats.get("needs_ocr", 0))
col_c.metric("En review", stats.get("needs_workflow_review", 0))

st.info("La carpeta de entrada debe respetar la estructura: data/input/A|B|C/01..99")

default_folder = os.getenv("COVERS_DIR", "data/input")

with st.form("ingest_form"):
    folder = st.text_input("Carpeta de imágenes", value=default_folder)
    recursive = st.checkbox("Recursivo dentro del módulo", value=True)
    overwrite_paths = st.checkbox("Sobrescribir rutas ya registradas", value=False)
    ext_text = st.text_input("Extensiones (coma separadas)", value="jpg,jpeg,png,webp,heic")

    submitted = st.form_submit_button("Extraer módulo", type="primary")

if submitted:
    extensions = [item.strip() for item in ext_text.split(",") if item.strip()]
    payload = {
        "folder": folder,
        "block": scope_block,
        "module": scope_module,
        "recursive": recursive,
        "extensions": extensions,
        "overwrite_existing_paths": overwrite_paths,
    }
    try:
        result = api_post("/covers/ingest", json=payload, timeout=900.0)
        st.success("Extracción completada")
        st.json(result)
    except Exception as exc:
        st.error(f"Error en extracción: {exc}")

st.subheader("Vista rápida de libros")

stage_filter = st.selectbox(
    "Filtrar por etapa",
    ["(todas)", "ocr", "metadata", "catalog", "cover", "review", "done", "needs_workflow_review"],
)
limit = st.number_input("Límite", min_value=1, max_value=5000, value=200)

try:
    params = {"limit": int(limit), **scope_params(scope_block, scope_module)}
    if stage_filter != "(todas)":
        params["stage"] = stage_filter
    rows = api_get("/books", params=params, timeout=20.0)
    df = pd.DataFrame(rows)
    if not df.empty:
        preview_cols = [
            col
            for col in [
                "id",
                "block",
                "module",
                "pipeline_stage",
                "workflow_status",
                "workflow_needs_review",
                "ocr_status",
                "metadata_status",
                "catalog_status",
                "cover_status",
                "image_count",
                "updated_at",
            ]
            if col in df.columns
        ]
        st.dataframe(df[preview_cols], width="stretch", hide_index=True)
    else:
        st.info("No hay registros para el filtro actual.")
except Exception as exc:
    st.error(f"No se pudo cargar /books: {exc}")
