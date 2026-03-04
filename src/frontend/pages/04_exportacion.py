from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        api_get,
        configure_page,
        scope_params,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        api_get,
        configure_page,
        scope_params,
        select_module_scope,
        show_backend_status,
    )

configure_page("Exportacion | Media Catalog Books")

st.title("Fase 4 · Exportacion")
show_backend_status()

scope_block, scope_module = select_module_scope(key_prefix="export_scope", title="Modulo de trabajo")
if not scope_module:
    st.stop()

st.write("Genera un TSV consolidado desde DuckDB (`exports/books.tsv`).")

if st.button("Exportar TSV", type="primary"):
    try:
        result = api_get("/export/books/tsv", timeout=120.0)
        path = Path(str(result.get("path") or "exports/books.tsv"))
        st.success(f"Exportado en: {path}")
        if path.exists():
            st.caption(f"Tamano: {path.stat().st_size} bytes")
    except Exception as exc:
        st.error(f"Error exportando TSV: {exc}")

st.subheader("Preview de libros del modulo")
try:
    rows = api_get("/books", params={"limit": 300, **scope_params(scope_block, scope_module)}, timeout=20.0)
    if rows:
        df = pd.DataFrame(rows)
        preview_cols = [
            col
            for col in [
                "id",
                "block",
                "module",
                "pipeline_stage",
                "workflow_status",
                "catalog_status",
                "cover_status",
                "cover_path",
                "updated_at",
            ]
            if col in df.columns
        ]
        st.dataframe(df[preview_cols], width="stretch", hide_index=True)
    else:
        st.info("No hay libros para previsualizar en este modulo.")
except Exception as exc:
    st.error(f"No se pudo cargar la preview: {exc}")
