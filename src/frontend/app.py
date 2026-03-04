import streamlit as st

try:
    from src.frontend.utils import (
        configure_page,
        get_selected_book_id,
        get_selected_scope,
        load_stats,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        configure_page,
        get_selected_book_id,
        get_selected_scope,
        load_stats,
        show_backend_status,
    )

configure_page()

st.title("Media Catalog Books")
st.caption("Refactor de book_catalog_v0.3 con FastAPI + LangGraph + DuckDB")

st.markdown(
    """
Pipeline operativo:

0. Orquestacion LangGraph
1. Ingesta de imagenes de creditos
2. OCR + ISBN
3. Metadatos de APIs
4. Consolidacion catalografica
5. Portada y exportacion
"""
)

show_backend_status()
active_block, active_module = get_selected_scope()
if active_module:
    st.caption(f"Scope activo: {active_block}/{active_module}")
else:
    st.caption(f"Scope activo: {active_block} (sin modulos)")
stats = load_stats(block=active_block, module=active_module)

col1, col2, col3 = st.columns(3)
col1.metric("Total", stats.get("total", 0))
col2.metric("En review", stats.get("needs_workflow_review", 0))
col3.metric("Pend. OCR", stats.get("needs_ocr", 0))

selected_book = get_selected_book_id()
if selected_book:
    st.sidebar.caption(f"Libro seleccionado: {selected_book}")

st.info("Usa el menu lateral para recorrer cada fase del proceso.")
