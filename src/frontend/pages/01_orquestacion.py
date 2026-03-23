import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        API_URL,
        CATALOG_OLLAMA_MODEL_DEFAULT,
        CATALOG_OLLAMA_MODEL_SUGGESTIONS,
        CATALOG_OPENAI_MODEL_DEFAULT,
        CATALOG_PROVIDER_DEFAULT,
        OCR_OLLAMA_MODEL_DEFAULT,
        OCR_OLLAMA_MODEL_SUGGESTIONS,
        OCR_OPENAI_MODEL_DEFAULT,
        OCR_PROVIDER_DEFAULT,
        OCR_RESIZE_TO_1800_DEFAULT,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
        render_ollama_model_selector,
        seed_widget_once,
        scope_params,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        API_URL,
        CATALOG_OLLAMA_MODEL_DEFAULT,
        CATALOG_OLLAMA_MODEL_SUGGESTIONS,
        CATALOG_OPENAI_MODEL_DEFAULT,
        CATALOG_PROVIDER_DEFAULT,
        OCR_OLLAMA_MODEL_DEFAULT,
        OCR_OLLAMA_MODEL_SUGGESTIONS,
        OCR_OPENAI_MODEL_DEFAULT,
        OCR_PROVIDER_DEFAULT,
        OCR_RESIZE_TO_1800_DEFAULT,
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
        render_ollama_model_selector,
        seed_widget_once,
        scope_params,
        select_module_scope,
        show_backend_status,
    )

configure_page("Orquestacion | Media Catalog Books")

st.title("Fase 1 · Orquestacion LangGraph")
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

STAGE_INDEX = {stage_name: idx for idx, stage_name in enumerate(WORKFLOW_STAGES)}

col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
with col1:
    selected_id = st.text_input("Book ID (opcional)", value="", placeholder="03B0001")
with col2:
    start_stage_key = "orq_start_stage"
    if start_stage_key not in st.session_state:
        st.session_state[start_stage_key] = WORKFLOW_STAGES[0]
    start_stage = st.selectbox("Desde", WORKFLOW_STAGES, key=start_stage_key)
with col3:
    stop_options = ["(sin limite)"] + list(WORKFLOW_STAGES)
    stop_after_key = "orq_stop_after"
    prev_start_key = "orq_prev_start_stage"
    previous_start = str(st.session_state.get(prev_start_key) or "").strip()
    if stop_after_key not in st.session_state:
        st.session_state[stop_after_key] = start_stage
    elif previous_start != start_stage:
        # When start stage changes, default stop stage to the same stage.
        st.session_state[stop_after_key] = start_stage
    if st.session_state.get(stop_after_key) not in stop_options:
        st.session_state[stop_after_key] = start_stage
    stop_after = st.selectbox("Parar en", stop_options, key=stop_after_key)
    st.session_state[prev_start_key] = start_stage
with col4:
    st.caption("El limite de lote depende de la etapa y overwrite")

overwrite = st.checkbox("Sobrescribir etapas ya completas", value=False)
max_attempts = st.number_input("Reintentos maximos", min_value=0, max_value=20, value=2)

start_idx = STAGE_INDEX.get(start_stage, 0)
stop_idx = len(WORKFLOW_STAGES) - 1 if stop_after == "(sin limite)" else STAGE_INDEX.get(stop_after, start_idx)
if stop_idx < start_idx:
    stop_idx = start_idx

ocr_in_flow = STAGE_INDEX["ocr"] >= start_idx and STAGE_INDEX["ocr"] <= stop_idx
catalog_in_flow = STAGE_INDEX["catalog"] >= start_idx and STAGE_INDEX["catalog"] <= stop_idx

eligible_limit: int | None = None
if not overwrite:
    try:
        eligible_payload = api_get(
            "/workflow/eligible",
            params={
                "start_stage": start_stage,
                "overwrite": "false",
                **scope_params(scope_block, scope_module),
            },
            timeout=10.0,
        )
        eligible_limit = int(eligible_payload.get("eligible", 0))
    except Exception as exc:
        st.warning(f"No se pudo calcular elegibles para el lote: {exc}")
        eligible_limit = None

if overwrite:
    limit = st.number_input("Lote", min_value=1, max_value=5000, value=20)
else:
    if eligible_limit is None:
        limit = st.number_input("Lote", min_value=1, max_value=5000, value=20)
    elif eligible_limit <= 0:
        st.info(f"No hay items elegibles en etapa '{start_stage}' para el modulo seleccionado.")
        limit = 0
    else:
        st.caption(f"Elegibles exactos para '{start_stage}' sin overwrite: {eligible_limit}")
        limit = st.number_input(
            "Lote",
            min_value=1,
            max_value=int(eligible_limit),
            value=min(20, int(eligible_limit)),
        )

col_provider, col_model = st.columns([1, 2])
with col_provider:
    ocr_provider_options = ["openai", "ollama"]
    ocr_provider_index = 0
    if OCR_PROVIDER_DEFAULT in ocr_provider_options:
        ocr_provider_index = ocr_provider_options.index(OCR_PROVIDER_DEFAULT)
    seed_widget_once("orq_ocr_provider", ocr_provider_options[ocr_provider_index])
    ocr_provider = st.selectbox(
        "OCR provider",
        ocr_provider_options,
        key="orq_ocr_provider",
        disabled=not ocr_in_flow,
    )
with col_model:
    try:
        ollama_models = load_ollama_models()
    except Exception as exc:
        ollama_models = []
        st.warning(f"No se pudieron cargar modelos de Ollama: {exc}")

    if ocr_provider == "ollama":
        ocr_model = render_ollama_model_selector(
            label="Modelo OCR",
            key="orq_ocr_model_ollama",
            installed_models=ollama_models,
            default_model=OCR_OLLAMA_MODEL_DEFAULT,
            suggested_models=OCR_OLLAMA_MODEL_SUGGESTIONS,
            disabled=not ocr_in_flow,
        )
    else:
        seed_widget_once("orq_ocr_model_openai", OCR_OPENAI_MODEL_DEFAULT)
        ocr_model = st.text_input(
            "Modelo OCR",
            placeholder=OCR_OPENAI_MODEL_DEFAULT,
            key="orq_ocr_model_openai",
            disabled=not ocr_in_flow,
        )

    seed_widget_once("orq_ocr_resize_to_1800", OCR_RESIZE_TO_1800_DEFAULT)
    ocr_resize_to_1800 = st.checkbox(
        "Reducir imagen a 1800 px (solo para glm-ocr)",
        key="orq_ocr_resize_to_1800",
        help="Si esta activo y el modelo OCR empieza por 'glm-ocr', se redimensiona la imagen al lado maximo 1800.",
        disabled=not ocr_in_flow,
    )
    if not ocr_in_flow:
        st.caption("OCR fuera del rango seleccionado; configuración desactivada.")

st.caption("Configuracion de catalogacion automatica")
cat_col_a, cat_col_b = st.columns([1, 2])
with cat_col_a:
    catalog_provider_options = ["openai", "ollama"]
    catalog_provider_index = 0
    if CATALOG_PROVIDER_DEFAULT in catalog_provider_options:
        catalog_provider_index = catalog_provider_options.index(CATALOG_PROVIDER_DEFAULT)
    seed_widget_once("orq_catalog_provider", catalog_provider_options[catalog_provider_index])
    catalog_provider = st.selectbox(
        "Provider catalogo",
        catalog_provider_options,
        key="orq_catalog_provider",
        disabled=not catalog_in_flow,
    )
with cat_col_b:
    if catalog_provider == "ollama":
        catalog_model = render_ollama_model_selector(
            label="Modelo catalogo",
            key="orq_catalog_model_ollama",
            installed_models=ollama_models,
            default_model=CATALOG_OLLAMA_MODEL_DEFAULT,
            suggested_models=CATALOG_OLLAMA_MODEL_SUGGESTIONS,
            disabled=not catalog_in_flow,
        )
    else:
        seed_widget_once("orq_catalog_model_openai", CATALOG_OPENAI_MODEL_DEFAULT)
        catalog_model = st.text_input(
            "Modelo catalogo",
            placeholder=CATALOG_OPENAI_MODEL_DEFAULT,
            key="orq_catalog_model_openai",
            disabled=not catalog_in_flow,
        )
if not catalog_in_flow:
    st.caption("Catalogación fuera del rango seleccionado; configuración desactivada.")

if ocr_in_flow:
    st.caption(f"OCR efectivo (si no tocas nada): `{ocr_provider}` / `{ocr_model}`")
if catalog_in_flow:
    st.caption(f"Catalog efectivo (si no tocas nada): `{catalog_provider}` / `{catalog_model}`")

if st.button("Ejecutar workflow", type="primary"):
    if not overwrite and int(limit) <= 0:
        st.warning("No hay items elegibles para ejecutar con esa etapa inicial y overwrite desactivado.")
        st.stop()

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
        "ocr_model": (ocr_model.strip() or None) if ocr_in_flow else None,
        "ocr_resize_to_1800": bool(ocr_resize_to_1800) if ocr_in_flow else False,
        "catalog_provider": catalog_provider if catalog_in_flow else None,
        "catalog_model": (catalog_model.strip() or None) if catalog_in_flow else None,
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

st.subheader("Items en running")
running_total = int(stage_counts.get("running", 0))
if running_total > 0:
    try:
        running_rows = api_get(
            "/books",
            params={"limit": 5000, **scope_params(scope_block, scope_module)},
            timeout=20.0,
        )
        running_items = [
            row
            for row in running_rows
            if str(row.get("workflow_status") or "").strip().lower() == "running"
        ]
        if running_items:
            running_table = []
            for row in running_items:
                node = str(row.get("workflow_current_node") or "").strip()
                stage = str(row.get("pipeline_stage") or "").strip()
                workflow_action = str(row.get("workflow_action") or "").strip()

                llm_value = ""
                if "llm=" in workflow_action:
                    llm_value = workflow_action.split("llm=", maxsplit=1)[1].strip()
                    if "|" in llm_value:
                        llm_value = llm_value.split("|", maxsplit=1)[0].strip()

                if not llm_value and node == "ocr":
                    provider = str(row.get("ocr_provider") or "").strip()
                    model = str(row.get("ocr_model") or "").strip()
                    if provider and model:
                        llm_value = f"{provider}/{model}"
                    elif provider or model:
                        llm_value = provider or model

                running_table.append(
                    {
                        "id": str(row.get("id") or ""),
                        "nodo": node or "(sin nodo)",
                        "etapa": stage or "(sin etapa)",
                        "accion": workflow_action or (f"Ejecutando {node}" if node else "Ejecutando workflow"),
                        "llm": llm_value or "-",
                        "attempt": int(row.get("workflow_attempt") or 0),
                        "updated_at": row.get("updated_at"),
                    }
                )
            st.dataframe(pd.DataFrame(running_table), width="stretch", hide_index=True)
        else:
            st.info("No hay items en running ahora mismo.")
    except Exception as exc:
        st.error(f"No se pudo cargar el detalle de running: {exc}")
else:
    st.success("No hay items en running.")

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
                payload["ocr_resize_to_1800"] = bool(ocr_resize_to_1800)
                payload["catalog_provider"] = catalog_provider
                payload["catalog_model"] = catalog_model.strip() or None
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
