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


def _render_home() -> None:
    configure_page()

    st.markdown(
        """
    <div class="mc-hero">
      <div class="mc-kicker">MC Books</div>
      <h1 class="mc-hero-title">Catalogación por bloques, módulo a módulo</h1>
      <p class="mc-hero-sub">
        Recorre el trabajo completo por módulo: prepara las imágenes, obtiene los datos,
        revisa cada libro y genera la exportación final.
      </p>
      <div class="mc-stage-grid">
        <div class="mc-stage-card"><span class="mc-stage-index">0</span><span class="mc-stage-title">Extracción</span></div>
        <div class="mc-stage-card"><span class="mc-stage-index">1</span><span class="mc-stage-title">Orquestación</span></div>
        <div class="mc-stage-card"><span class="mc-stage-index">2</span><span class="mc-stage-title">Revisión OCR + ISBN</span></div>
        <div class="mc-stage-card"><span class="mc-stage-index">3</span><span class="mc-stage-title">Formulario</span></div>
        <div class="mc-stage-card"><span class="mc-stage-index">4</span><span class="mc-stage-title">Exportación</span></div>
        <div class="mc-stage-card"><span class="mc-stage-index">5</span><span class="mc-stage-title">Datos</span></div>
      </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    show_backend_status()
    active_block, active_module = get_selected_scope()
    if active_module:
        st.caption(f"Módulo activo: {active_block}/{active_module}")
    else:
        st.caption(f"Bloque activo: {active_block} (sin módulos)")
    stats = load_stats(block=active_block, module=active_module)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total", stats.get("total", 0))
    col2.metric("En revisión", stats.get("needs_workflow_review", 0))
    col3.metric("Pendientes de OCR", stats.get("needs_ocr", 0))
    col4.metric("Consolidadas", stats.get("form_consolidated", 0))

    selected_book = get_selected_book_id()
    if selected_book:
        st.sidebar.caption(f"Libro seleccionado: {selected_book}")

    st.caption("Usa el menú lateral para abrir la fase en la que quieras trabajar.")


pages = [
    st.Page(
        _render_home,
        title="Inicio",
        icon=":material/home:",
        default=True,
    ),
    st.Page(
        "pages/00_extraccion.py",
        title="Extracción",
        icon=":material/add_photo_alternate:",
        url_path="extraccion",
    ),
    st.Page(
        "pages/01_orquestacion.py",
        title="Orquestación",
        icon=":material/account_tree:",
        url_path="orquestacion",
    ),
    st.Page(
        "pages/02_revision_manual.py",
        title="Revisión OCR e ISBN",
        icon=":material/fact_check:",
        url_path="revision_manual",
    ),
    st.Page(
        "pages/03_formulario.py",
        title="Formulario",
        icon=":material/menu_book:",
        url_path="formulario",
    ),
    st.Page(
        "pages/04_exportacion.py",
        title="Exportación",
        icon=":material/upload_file:",
        url_path="exportacion",
    ),
    st.Page(
        "pages/05_datos.py",
        title="Datos",
        icon=":material/database:",
        url_path="datos",
    ),
]

current_page = st.navigation(pages, position="sidebar", expanded=True)
current_page.run()
