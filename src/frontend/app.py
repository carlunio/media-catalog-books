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

st.markdown(
    """
<div class="mc-hero">
  <div class="mc-kicker">Media Catalog Books</div>
  <h1 class="mc-hero-title">Catalogacion por bloques, modulo a modulo</h1>
  <p class="mc-hero-sub">
    Opera la cadena completa con FastAPI + LangGraph + DuckDB:
    ingesta, OCR, ISBN, metadatos y consolidacion.
  </p>
  <div class="mc-stage-grid">
    <div class="mc-stage-card"><span class="mc-stage-index">0</span><span class="mc-stage-title">Extraccion</span></div>
    <div class="mc-stage-card"><span class="mc-stage-index">1</span><span class="mc-stage-title">Orquestacion</span></div>
    <div class="mc-stage-card"><span class="mc-stage-index">2</span><span class="mc-stage-title">Revision OCR + ISBN</span></div>
    <div class="mc-stage-card"><span class="mc-stage-index">3</span><span class="mc-stage-title">Formulario</span></div>
    <div class="mc-stage-card"><span class="mc-stage-index">4</span><span class="mc-stage-title">Exportacion</span></div>
  </div>
</div>
""",
    unsafe_allow_html=True,
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

st.info("Usa el menu lateral para entrar en cada fase del flujo.")
