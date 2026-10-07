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

configure_page("Orquestación | Media Catalog Books")

st.title("Fase 1 · Orquestación LangGraph")
st.caption(f"Backend objetivo: {API_URL}")
show_backend_status()

scope_block, scope_module = select_module_scope(
    key_prefix="orq_scope", title="Módulo de trabajo"
)
if not scope_module:
    st.stop()

with st.expander("Definición del grafo", expanded=False):
    try:
        graph = api_get("/workflow/graph", timeout=12.0)
        st.write("LangGraph disponible:", graph.get("langgraph_available"))
        col_a, col_b = st.columns(2)
        with col_a:
            st.write("Nodos")
            st.dataframe(
                pd.DataFrame(graph.get("nodes", [])), width="stretch", hide_index=True
            )
        with col_b:
            st.write("Aristas")
            st.dataframe(
                pd.DataFrame(graph.get("edges", [])), width="stretch", hide_index=True
            )
    except Exception as exc:
        st.error(f"No se pudo cargar /workflow/graph: {exc}")

st.subheader("Ejecución del flujo de trabajo")

STAGE_INDEX = {stage_name: idx for idx, stage_name in enumerate(WORKFLOW_STAGES)}

col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
with col1:
    selected_id = st.text_input(
        "ID del libro (opcional)", value="", placeholder="03B0001"
    )
with col2:
    start_stage_key = "orq_start_stage"
    if start_stage_key not in st.session_state:
        st.session_state[start_stage_key] = WORKFLOW_STAGES[0]
    start_stage = st.selectbox("Desde", WORKFLOW_STAGES, key=start_stage_key)
with col3:
    stop_options = ["(sin límite)"] + list(WORKFLOW_STAGES)
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
    st.caption("El límite del lote se calcula con los libros elegibles")

overwrite = st.checkbox(
    "Sobrescribir etapas ya completas",
    value=False,
    help=(
        "Permite repetir la etapa elegida en libros que ya han avanzado más. "
        "Los libros con el flujo completado, consolidados, en revisión o en ejecución siempre "
        "quedan excluidos."
    ),
)
max_attempts = st.number_input("Reintentos máximos", min_value=0, max_value=20, value=2)
st.caption(
    "El lote no tiene un tiempo de espera global. Cada llamada externa tiene su propio "
    "límite y el contador se reinicia para el siguiente libro."
)

start_idx = STAGE_INDEX.get(start_stage, 0)
stop_idx = (
    len(WORKFLOW_STAGES) - 1
    if stop_after == "(sin límite)"
    else STAGE_INDEX.get(stop_after, start_idx)
)
if stop_idx < start_idx:
    stop_idx = start_idx

ocr_in_flow = STAGE_INDEX["ocr"] >= start_idx and STAGE_INDEX["ocr"] <= stop_idx
metadata_in_flow = (
    STAGE_INDEX["metadata"] >= start_idx and STAGE_INDEX["metadata"] <= stop_idx
)
catalog_in_flow = (
    STAGE_INDEX["catalog"] >= start_idx and STAGE_INDEX["catalog"] <= stop_idx
)

eligible_limit: int | None = None
try:
    eligible_payload = api_get(
        "/workflow/eligible",
        params={
            "start_stage": start_stage,
            "overwrite": str(bool(overwrite)).lower(),
            **scope_params(scope_block, scope_module),
        },
        timeout=10.0,
    )
    eligible_limit = int(eligible_payload.get("eligible", 0))
except Exception as exc:
    st.warning(f"No se pudo calcular elegibles para el lote: {exc}")
    eligible_limit = None

if eligible_limit is None:
    limit = st.number_input("Lote", min_value=1, max_value=5000, value=20)
elif eligible_limit <= 0:
    overwrite_text = " con sobrescritura" if overwrite else ""
    st.info(
        f"No hay elementos elegibles para '{start_stage}'{overwrite_text} "
        "en el módulo seleccionado. Si el rango incluye metadatos y mantienes "
        "activada la descarga automática, aún puedes ejecutar el barrido de "
        "portadas pendientes."
    )
    limit = 0
else:
    eligibility_text = (
        "incluyendo etapas posteriores" if overwrite else "en la etapa exacta"
    )
    st.caption(f"Elegibles para '{start_stage}' ({eligibility_text}): {eligible_limit}")
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
        "Proveedor de OCR",
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
        help="Si está activo y el modelo OCR empieza por 'glm-ocr', la imagen se reduce a un lado máximo de 1800 px.",
        disabled=not ocr_in_flow,
    )
    if not ocr_in_flow:
        st.caption("OCR fuera del rango seleccionado; configuración desactivada.")

download_cover_after_metadata = st.checkbox(
    "Descargar portadas al obtener metadatos",
    value=True,
    help=(
        "Después de consultar las API, descarga la portada si ese ID todavía "
        "no tiene una. Un fallo de portada no detiene el flujo de trabajo."
    ),
    disabled=not metadata_in_flow,
)
if not metadata_in_flow:
    st.caption(
        "Metadatos fuera del rango seleccionado; descarga automática desactivada."
    )

st.caption("Configuración de catalogación automática")
cat_col_a, cat_col_b = st.columns([1, 2])
with cat_col_a:
    catalog_provider_options = ["openai", "ollama"]
    catalog_provider_index = 0
    if CATALOG_PROVIDER_DEFAULT in catalog_provider_options:
        catalog_provider_index = catalog_provider_options.index(
            CATALOG_PROVIDER_DEFAULT
        )
    seed_widget_once(
        "orq_catalog_provider", catalog_provider_options[catalog_provider_index]
    )
    catalog_provider = st.selectbox(
        "Proveedor del catálogo",
        catalog_provider_options,
        key="orq_catalog_provider",
        disabled=not catalog_in_flow,
    )
with cat_col_b:
    if catalog_provider == "ollama":
        catalog_model = render_ollama_model_selector(
            label="Modelo del catálogo",
            key="orq_catalog_model_ollama",
            installed_models=ollama_models,
            default_model=CATALOG_OLLAMA_MODEL_DEFAULT,
            suggested_models=CATALOG_OLLAMA_MODEL_SUGGESTIONS,
            disabled=not catalog_in_flow,
        )
    else:
        seed_widget_once("orq_catalog_model_openai", CATALOG_OPENAI_MODEL_DEFAULT)
        catalog_model = st.text_input(
            "Modelo del catálogo",
            placeholder=CATALOG_OPENAI_MODEL_DEFAULT,
            key="orq_catalog_model_openai",
            disabled=not catalog_in_flow,
        )
if not catalog_in_flow:
    st.caption("Catalogación fuera del rango seleccionado; configuración desactivada.")

if ocr_in_flow:
    st.caption(f"OCR efectivo (si no tocas nada): `{ocr_provider}` / `{ocr_model}`")
if catalog_in_flow:
    st.caption(
        f"Catálogo efectivo (si no tocas nada): `{catalog_provider}` / `{catalog_model}`"
    )

if st.button("Ejecutar flujo", type="primary"):
    cover_sweep_requested = metadata_in_flow and bool(download_cover_after_metadata)
    if int(limit) <= 0 and not cover_sweep_requested:
        st.warning("No hay elementos elegibles para ejecutar con esa configuración.")
        st.stop()

    payload = {
        "book_id": selected_id.strip() or None,
        "block": scope_block,
        "module": scope_module,
        "limit": max(1, int(limit)),
        "start_stage": start_stage,
        "stop_after": None if stop_after == "(sin límite)" else stop_after,
        "overwrite": bool(overwrite),
        "download_cover_after_metadata": (
            bool(download_cover_after_metadata) if metadata_in_flow else False
        ),
        "max_attempts": int(max_attempts),
        "ocr_provider": ocr_provider,
        "ocr_model": (ocr_model.strip() or None) if ocr_in_flow else None,
        "ocr_resize_to_1800": bool(ocr_resize_to_1800) if ocr_in_flow else False,
        "catalog_provider": catalog_provider if catalog_in_flow else None,
        "catalog_model": (catalog_model.strip() or None) if catalog_in_flow else None,
    }
    try:
        result = api_post("/workflow/run", json=payload, timeout=None)
        st.success(
            f"Procesados {result.get('processed', 0)} de {result.get('requested', 0)}"
        )
        items = result.get("items", [])
        if items:
            st.dataframe(pd.DataFrame(items), width="stretch", hide_index=True)
        cover_sweep = result.get("cover_sweep")
        if isinstance(cover_sweep, dict):
            st.info(
                "Barrido de portadas del módulo: "
                f"{cover_sweep.get('processed', 0)} pendientes procesadas."
            )
            cover_items = cover_sweep.get("items", [])
            if cover_items:
                st.dataframe(
                    pd.DataFrame(cover_items),
                    width="stretch",
                    hide_index=True,
                )
    except Exception as exc:
        st.error(f"Error al ejecutar el flujo: {exc}")

st.divider()
st.subheader("Descarga de portadas")
st.caption(
    "Rama opcional basada en las imágenes de las API. Se puede ejecutar en "
    "cualquier momento, también después de consolidar la ficha, y no modifica "
    "su estado ni bloquea los pasos del flujo de trabajo."
)

try:
    cover_eligible_payload = api_get(
        "/workflow/eligible",
        params={
            "start_stage": "cover",
            "overwrite": "false",
            **scope_params(scope_block, scope_module),
        },
        timeout=10.0,
    )
    cover_eligible = int(cover_eligible_payload.get("eligible", 0))
    st.caption(f"Portadas pendientes en el módulo: {cover_eligible}")
except Exception as exc:
    st.warning(f"No se pudo calcular las portadas pendientes: {exc}")

with st.form("orq_cover_download_form", border=True):
    cover_id_col, cover_limit_col = st.columns([2, 1])
    with cover_id_col:
        cover_book_id = st.text_input(
            "ID del libro (opcional)",
            placeholder="03B0001",
            key="orq_cover_book_id",
        )
    with cover_limit_col:
        cover_limit = st.number_input(
            "Lote de portadas",
            min_value=1,
            max_value=5000,
            value=20,
            key="orq_cover_limit",
        )
    cover_overwrite = st.checkbox(
        "Volver a descargar portadas ya procesadas",
        value=False,
        key="orq_cover_overwrite",
    )
    download_covers = st.form_submit_button(
        "Descargar portadas",
        icon=":material/download:",
        type="primary",
    )

if download_covers:
    try:
        cover_result = api_post(
            "/cover/download",
            json={
                "book_id": cover_book_id.strip() or None,
                "block": scope_block,
                "module": scope_module,
                "limit": int(cover_limit),
                "overwrite": bool(cover_overwrite),
            },
            timeout=None,
        )
        st.success(
            "Rama de portadas completada: "
            f"{cover_result.get('processed', 0)} de "
            f"{cover_result.get('requested', 0)} procesadas."
        )
        cover_items = cover_result.get("items", [])
        if cover_items:
            st.dataframe(pd.DataFrame(cover_items), width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"Error descargando portadas: {exc}")

st.subheader("Estado operativo")

if st.button("Actualizar estado"):
    st.cache_data.clear()

try:
    params = {
        "limit": 5000,
        "review_limit": 300,
        **scope_params(scope_block, scope_module),
    }
    snapshot = api_get("/workflow/snapshot", params=params, timeout=12.0)
except Exception as exc:
    st.error(f"No se pudo cargar el estado: {exc}")
    st.stop()

stage_counts = snapshot.get("stage_counts", {})
workflow_status_counts = snapshot.get("workflow_status_counts", {})
form_status_counts = snapshot.get("form_status_counts", {})
running_nodes = snapshot.get("running_nodes", {})

m1, m2, m3, m4 = st.columns(4)
m1.metric("OCR", int(stage_counts.get("ocr", 0)))
m2.metric("Metadatos", int(stage_counts.get("metadata", 0)))
m3.metric("Catálogo", int(stage_counts.get("catalog", 0)))
m4.metric("Portadas", int(stage_counts.get("cover", 0)))

m5, m6, m7, m8 = st.columns(4)
m5.metric("Revisión", int(stage_counts.get("review", 0)))
m6.metric("Completados", int(stage_counts.get("done", 0)))
m7.metric("En ejecución", int(stage_counts.get("running", 0)))
m8.metric("Desconocidos", int(stage_counts.get("unknown", 0)))

with st.expander("Detalle de estados", expanded=False):
    st.write("Recuento por estado del flujo")
    st.dataframe(
        pd.DataFrame([workflow_status_counts]), width="stretch", hide_index=True
    )
    st.write("Recuento por estado del formulario")
    st.dataframe(pd.DataFrame([form_status_counts]), width="stretch", hide_index=True)
    st.write("Nodos en ejecución")
    st.dataframe(pd.DataFrame([running_nodes]), width="stretch", hide_index=True)

st.subheader("Elementos en ejecución")
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
                        "acción": workflow_action
                        or (f"Ejecutando {node}" if node else "Ejecutando flujo"),
                        "llm": llm_value or "-",
                        "intento": int(row.get("workflow_attempt") or 0),
                        "updated_at": row.get("updated_at"),
                    }
                )
            st.dataframe(pd.DataFrame(running_table), width="stretch", hide_index=True)
        else:
            st.info("No hay elementos en ejecución ahora mismo.")
    except Exception as exc:
        st.error(f"No se pudo cargar el detalle de la ejecución: {exc}")
else:
    st.success("No hay elementos en ejecución.")

review_queue = snapshot.get("review_queue", [])
st.subheader("Cola de revisión")

if review_queue:
    queue_df = pd.DataFrame(review_queue)
    st.dataframe(queue_df, width="stretch", hide_index=True)

    ids = [str(item.get("id") or "").strip() for item in review_queue]
    ids = [item for item in ids if item]

    selected_review_id = st.selectbox("Libro en revisión", ids, key="review_book_id")
    action = st.selectbox(
        "Acción",
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
        if st.button("Aplicar acción de revisión"):
            try:
                payload = {"action": action, "max_attempts": int(max_attempts)}
                payload["ocr_provider"] = ocr_provider
                payload["ocr_model"] = ocr_model.strip() or None
                payload["ocr_resize_to_1800"] = bool(ocr_resize_to_1800)
                payload["catalog_provider"] = catalog_provider
                payload["catalog_model"] = catalog_model.strip() or None
                result = api_post(
                    f"/workflow/review/{selected_review_id}",
                    json=payload,
                    timeout=None,
                )
                st.success("Acción aplicada")
                st.json(result)
            except Exception as exc:
                st.error(f"No se pudo aplicar la acción: {exc}")

    with col_mark:
        if st.button("Marcar de nuevo para revisión"):
            try:
                payload = {
                    "reason": "Marcado manual desde la página de orquestación",
                    "node": "manual",
                }
                result = api_post(
                    f"/workflow/review/{selected_review_id}/mark", json=payload
                )
                st.success("Libro marcado para revisión")
                st.json(result)
            except Exception as exc:
                st.error(f"No se pudo marcar para revisión: {exc}")
else:
    st.success("No hay libros en cola de revisión.")
