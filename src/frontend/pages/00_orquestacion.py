import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        API_URL,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
        scope_params,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        API_URL,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
        scope_params,
        select_module_scope,
        show_backend_status,
    )

configure_page("Orquestacion | Media Catalog Books")

st.title("Fase 0 · Orquestacion LangGraph")
st.caption(f"Backend objetivo: {API_URL}")
show_backend_status()

scope_block, scope_module = select_module_scope(key_prefix="orq_scope", title="Modulo de trabajo")
if not scope_module:
    st.stop()

with st.expander("Definicion del grafo", expanded=False):
    try:
        graph = api_get("/workflow/graph", timeout=12.0)
        st.write("LangGraph disponible:", graph.get("langgraph_available"))
        col_a, col_b = st.columns(2)
        with col_a:
            st.write("Nodos")
            st.dataframe(pd.DataFrame(graph.get("nodes", [])), width="stretch", hide_index=True)
        with col_b:
            st.write("Aristas")
            st.dataframe(pd.DataFrame(graph.get("edges", [])), width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"No se pudo cargar /workflow/graph: {exc}")

st.subheader("Ejecucion del pipeline")

col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
with col1:
    selected_id = st.text_input("Book ID (opcional)", value="", placeholder="03B0001")
with col2:
    start_stage = st.selectbox("Desde", WORKFLOW_STAGES, index=0)
with col3:
    stop_options = ["(sin limite)"] + list(WORKFLOW_STAGES)
    stop_after = st.selectbox("Parar en", stop_options, index=0)
with col4:
    limit = st.number_input("Lote", min_value=1, max_value=5000, value=20)

overwrite = st.checkbox("Sobrescribir etapas ya completas", value=False)
max_attempts = st.number_input("Reintentos maximos", min_value=0, max_value=20, value=2)

col_provider, col_model = st.columns([1, 2])
default_ollama_model = "glm-ocr:latest"
with col_provider:
    st.text_input("OCR provider", value="ollama", disabled=True)
    ocr_provider = "ollama"
with col_model:
    try:
        ollama_models = load_ollama_models()
    except Exception as exc:
        ollama_models = []
        st.warning(f"No se pudieron cargar modelos de Ollama: {exc}")

    options = [default_ollama_model] + [name for name in ollama_models if name != default_ollama_model]
    if len(options) > 1:
        ocr_model = st.selectbox("Modelo OCR", options, index=0)
    else:
        ocr_model = st.text_input("Modelo OCR", value=default_ollama_model, placeholder=default_ollama_model)

if st.button("Ejecutar workflow", type="primary"):
    payload = {
        "book_id": selected_id.strip() or None,
        "block": scope_block,
        "module": scope_module,
        "limit": int(limit),
        "start_stage": start_stage,
        "stop_after": None if stop_after == "(sin limite)" else stop_after,
        "overwrite": bool(overwrite),
        "max_attempts": int(max_attempts),
        "ocr_provider": ocr_provider,
        "ocr_model": ocr_model.strip() or None,
    }
    try:
        result = api_post("/workflow/run", json=payload, timeout=1800.0)
        st.success(f"Procesados {result.get('processed', 0)} de {result.get('requested', 0)}")
        items = result.get("items", [])
        if items:
            st.dataframe(pd.DataFrame(items), width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"Error ejecutando workflow: {exc}")

st.subheader("Snapshot operativo")

if st.button("Refrescar snapshot"):
    st.cache_data.clear()

try:
    params = {"limit": 5000, "review_limit": 300, **scope_params(scope_block, scope_module)}
    snapshot = api_get("/workflow/snapshot", params=params, timeout=12.0)
except Exception as exc:
    st.error(f"No se pudo cargar snapshot: {exc}")
    st.stop()

stage_counts = snapshot.get("stage_counts", {})
workflow_status_counts = snapshot.get("workflow_status_counts", {})
running_nodes = snapshot.get("running_nodes", {})

m1, m2, m3, m4 = st.columns(4)
m1.metric("OCR", int(stage_counts.get("ocr", 0)))
m2.metric("Metadata", int(stage_counts.get("metadata", 0)))
m3.metric("Catalog", int(stage_counts.get("catalog", 0)))
m4.metric("Cover", int(stage_counts.get("cover", 0)))

m5, m6, m7, m8 = st.columns(4)
m5.metric("Review", int(stage_counts.get("review", 0)))
m6.metric("Done", int(stage_counts.get("done", 0)))
m7.metric("Running", int(stage_counts.get("running", 0)))
m8.metric("Unknown", int(stage_counts.get("unknown", 0)))

with st.expander("Detalle de estados", expanded=False):
    st.write("Workflow status counts")
    st.dataframe(pd.DataFrame([workflow_status_counts]), width="stretch", hide_index=True)
    st.write("Running nodes")
    st.dataframe(pd.DataFrame([running_nodes]), width="stretch", hide_index=True)

review_queue = snapshot.get("review_queue", [])
st.subheader("Cola de revision")

if review_queue:
    queue_df = pd.DataFrame(review_queue)
    st.dataframe(queue_df, width="stretch", hide_index=True)

    ids = [str(item.get("id") or "").strip() for item in review_queue]
    ids = [item for item in ids if item]

    selected_review_id = st.selectbox("Libro en review", ids, key="review_book_id")
    action = st.selectbox(
        "Accion",
        [
            "approve",
            "retry_from_ocr",
            "retry_from_metadata",
            "retry_from_catalog",
            "retry_from_cover",
        ],
        key="review_action",
    )

    col_action, col_mark = st.columns(2)
    with col_action:
        if st.button("Aplicar accion de review"):
            try:
                payload = {"action": action, "max_attempts": int(max_attempts)}
                payload["ocr_provider"] = ocr_provider
                payload["ocr_model"] = ocr_model.strip() or None
                result = api_post(f"/workflow/review/{selected_review_id}", json=payload, timeout=600.0)
                st.success("Accion aplicada")
                st.json(result)
            except Exception as exc:
                st.error(f"No se pudo aplicar la accion: {exc}")

    with col_mark:
        if st.button("Marcar nuevamente en review"):
            try:
                payload = {
                    "reason": "Marcado manual desde pagina de orquestacion",
                    "node": "manual",
                }
                result = api_post(f"/workflow/review/{selected_review_id}/mark", json=payload)
                st.success("Libro marcado en review")
                st.json(result)
            except Exception as exc:
                st.error(f"No se pudo marcar en review: {exc}")
else:
    st.success("No hay libros en cola de revision.")
