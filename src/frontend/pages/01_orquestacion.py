import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
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
        render_page_header,
        load_ollama_models,
        render_ollama_model_selector,
        render_openai_catalog_model_selector,
        render_openai_ocr_model_selector,
        seed_widget_once,
        scope_params,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
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
        render_page_header,
        load_ollama_models,
        render_ollama_model_selector,
        render_openai_catalog_model_selector,
        render_openai_ocr_model_selector,
        seed_widget_once,
        scope_params,
        select_module_scope,
        show_backend_status,
    )

configure_page("Orquestación | MC Books")

render_page_header(
    phase="1",
    title="Orquestación",
    description=(
        "Consulta el estado del módulo y ejecuta OCR, metadatos, catalogación "
        "o la descarga de portadas."
    ),
)
show_backend_status()

scope_block, scope_module = select_module_scope(
    key_prefix="orq_scope", title="Módulo de trabajo"
)
if not scope_module:
    st.stop()

ACTIVE_JOB_STATUSES = {"queued", "running", "stopping"}
JOB_STATUS_LABELS = {
    "queued": "En cola",
    "running": "En ejecución",
    "stopping": "Deteniéndose",
    "stopped": "Detenida",
    "completed": "Completada",
    "failed": "Fallida",
}
JOB_PHASE_LABELS = {"workflow": "Flujo principal", "cover": "Portadas"}
STAGE_INDEX = {stage_name: idx for idx, stage_name in enumerate(WORKFLOW_STAGES)}
STAGE_LABELS = {"ocr": "OCR", "metadata": "Metadatos", "catalog": "Catalogación"}
LAST_JOB_SESSION_KEY = "orq_last_workflow_job_id"


def _job_stage_summary(job: dict) -> str:
    start_stage = str(job.get("start_stage") or "ocr")
    if start_stage == "cover":
        return "Portadas"

    start_index = STAGE_INDEX.get(start_stage, 0)
    stop_stage = str(job.get("stop_after") or WORKFLOW_STAGES[-1])
    stop_index = STAGE_INDEX.get(stop_stage, len(WORKFLOW_STAGES) - 1)
    stop_index = max(start_index, stop_index)
    labels = [
        STAGE_LABELS[stage]
        for stage in WORKFLOW_STAGES
        if start_index <= STAGE_INDEX[stage] <= stop_index
    ]
    summary = " → ".join(labels)
    metadata_index = STAGE_INDEX.get("metadata", -1)
    if (
        bool(job.get("download_cover_after_metadata"))
        and start_index <= metadata_index <= stop_index
    ):
        summary += " · barrido de portadas"
    return summary


def _job_progress_entries(job: dict) -> list[tuple[str, dict]]:
    progress = job.get("progress")
    if isinstance(progress, dict) and progress:
        return [
            (phase, progress[phase])
            for phase in ("workflow", "cover")
            if isinstance(progress.get(phase), dict)
        ]

    phase = str(job.get("phase") or "workflow")
    return [
        (
            phase,
            {
                "processed": job.get("processed"),
                "requested": job.get("requested"),
                "last_item": job.get("last_item"),
            },
        )
    ]


def _render_job_progress_panel(job: dict) -> None:
    status = str(job.get("status") or "")
    status_label = JOB_STATUS_LABELS.get(status, status or "Preparando")
    with st.container(border=True):
        title_col, status_col = st.columns([3, 1])
        with title_col:
            st.markdown("**Progreso de la ejecución**")
            st.caption(_job_stage_summary(job))
        with status_col:
            st.markdown(f"**{status_label}**")

        entries = _job_progress_entries(job)
        for phase, phase_progress in entries:
            phase_label = JOB_PHASE_LABELS.get(phase, phase.capitalize())
            processed = max(0, int(phase_progress.get("processed") or 0))
            requested = max(0, int(phase_progress.get("requested") or 0))
            unit = "portadas" if phase == "cover" else "libros"
            if requested > 0:
                fraction = min(1.0, processed / requested)
                progress_text = f"{processed}/{requested} {unit} procesados"
            elif status == "queued" and not job.get("progress"):
                fraction = 0.0
                progress_text = "Preparando el lote…"
            else:
                fraction = 1.0
                progress_text = f"0/0 {unit}: no había elementos elegibles"

            st.markdown(f"**{phase_label}**")
            st.progress(fraction, text=progress_text)

            item = phase_progress.get("last_item")
            if isinstance(item, dict) and item.get("id"):
                item_status = str(item.get("status") or "")
                prefix = "En curso" if item_status == "running" else "Último procesado"
                suffix = (
                    f" · {item_status}"
                    if item_status and item_status != "running"
                    else ""
                )
                item_id = str(item.get("id") or "")
                st.caption(f"{prefix}: `{item_id}`{suffix}")

        if status in ACTIVE_JOB_STATUSES:
            st.caption(
                "Pulsa Actualizar para consultar el último avance. Detener no inicia "
                "otra etapa y devuelve el libro a la última etapa válida guardada."
            )


active_job: dict | None = None
last_job: dict | None = None
try:
    current_job_payload = api_get(
        "/workflow/jobs/current",
        params=scope_params(scope_block, scope_module),
        timeout=10.0,
    )
    candidate = current_job_payload.get("job")
    if isinstance(candidate, dict):
        active_job = candidate
        st.session_state[LAST_JOB_SESSION_KEY] = str(candidate.get("job_id") or "")
except Exception as exc:
    st.warning(f"No se pudo consultar la ejecución activa: {exc}")

last_job_id = str(st.session_state.get(LAST_JOB_SESSION_KEY) or "").strip()
if last_job_id and (
    active_job is None or str(active_job.get("job_id") or "") != last_job_id
):
    try:
        last_job_payload = api_get(
            f"/workflow/jobs/{last_job_id}",
            timeout=10.0,
        )
        candidate = last_job_payload.get("job")
        if isinstance(candidate, dict):
            last_job = candidate
    except Exception:
        last_job = None
elif active_job is not None:
    last_job = active_job

st.subheader("Estado del módulo")
state_title_col, state_stop_col, state_refresh_col = st.columns([4, 1, 1])
with state_title_col:
    st.caption(
        "Situación actual antes de lanzar nuevas órdenes. Los recuentos se "
        "actualizan al recargar la página."
    )
with state_stop_col:
    stop_status = str((active_job or {}).get("status") or "")
    stop_disabled = active_job is None or stop_status == "stopping"
    stop_label = "Deteniendo" if stop_status == "stopping" else "Detener"
    if st.button(
        stop_label,
        icon=":material/stop_circle:",
        key="orq_stop_workflow",
        disabled=stop_disabled,
        width="stretch",
    ):
        try:
            stop_response = api_post(
                f"/workflow/jobs/{active_job['job_id']}/stop",
                timeout=15.0,
            )
            stopped_job = stop_response.get("job") or {}
            if str(stopped_job.get("status") or "") == "stopping":
                st.warning(
                    "Parada solicitada. La operación activa terminará de forma segura; "
                    "no se iniciará otra etapa y el libro volverá a su último "
                    "estado válido."
                )
            else:
                st.info("La ejecución ya había terminado.")
            st.rerun()
        except Exception as exc:
            st.error(f"No se pudo detener la ejecución: {exc}")
with state_refresh_col:
    st.button(
        "Actualizar",
        icon=":material/refresh:",
        key="orq_refresh_snapshot",
        width="stretch",
    )

if active_job is not None:
    _render_job_progress_panel(active_job)

snapshot: dict | None = None
try:
    snapshot = api_get(
        "/workflow/snapshot",
        params={
            "limit": 5000,
            "review_limit": 300,
            **scope_params(scope_block, scope_module),
        },
        timeout=12.0,
    )
except Exception as exc:
    st.warning(f"No se pudo cargar el estado del módulo: {exc}")

if snapshot is not None:
    stage_counts = snapshot.get("stage_counts", {})
    workflow_status_counts = snapshot.get("workflow_status_counts", {})
    form_status_counts = snapshot.get("form_status_counts", {})
    running_nodes = snapshot.get("running_nodes", {})
    review_queue = list(snapshot.get("review_queue") or [])

    overview_a, overview_b, overview_c, overview_d = st.columns(4)
    overview_a.metric("Libros del módulo", int(snapshot.get("total_considered") or 0))
    overview_b.metric("En ejecución", int(stage_counts.get("running", 0)))
    overview_c.metric("En revisión", int(stage_counts.get("review", 0)))
    overview_d.metric("Completados", int(stage_counts.get("done", 0)))

    stage_a, stage_b, stage_c, stage_d = st.columns(4)
    stage_a.metric("Pendientes de OCR", int(stage_counts.get("ocr", 0)))
    stage_b.metric("Pendientes de metadatos", int(stage_counts.get("metadata", 0)))
    stage_c.metric("Pendientes de catálogo", int(stage_counts.get("catalog", 0)))
    stage_d.metric("Pendientes de portada", int(stage_counts.get("cover", 0)))

    unknown_total = int(stage_counts.get("unknown", 0))
    if unknown_total:
        st.warning(f"Hay {unknown_total} libros con una etapa no reconocida.")

    running_total = int(stage_counts.get("running", 0))
    with st.expander(
        f"Elementos en ejecución · {running_total}",
        expanded=running_total > 0,
    ):
        if running_total > 0:
            try:
                running_rows = api_get(
                    "/books",
                    params={
                        "limit": 5000,
                        **scope_params(scope_block, scope_module),
                    },
                    timeout=20.0,
                )
                running_items = [
                    row
                    for row in running_rows
                    if str(row.get("workflow_status") or "").strip().lower()
                    == "running"
                ]
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
                        llm_value = (
                            f"{provider}/{model}"
                            if provider and model
                            else provider or model
                        )
                    running_table.append(
                        {
                            "Libro": str(row.get("id") or ""),
                            "Nodo": node or "(sin nodo)",
                            "Etapa": stage or "(sin etapa)",
                            "Acción": workflow_action
                            or (f"Ejecutando {node}" if node else "Ejecutando flujo"),
                            "Modelo": llm_value or "-",
                            "Intento": int(row.get("workflow_attempt") or 0),
                            "Actualizado": row.get("updated_at"),
                        }
                    )
                if running_table:
                    st.dataframe(
                        pd.DataFrame(running_table),
                        width="stretch",
                        hide_index=True,
                    )
                else:
                    st.info("No hay elementos en ejecución ahora mismo.")
            except Exception as exc:
                st.error(f"No se pudo cargar el detalle de la ejecución: {exc}")
        else:
            st.caption("No hay elementos en ejecución ahora mismo.")

    with st.expander(
        f"Cola de revisión · {len(review_queue)}",
        expanded=bool(review_queue),
    ):
        if review_queue:
            queue_df = pd.DataFrame(review_queue).rename(
                columns={
                    "id": "Libro",
                    "pipeline_stage": "Etapa",
                    "workflow_current_node": "Nodo",
                    "workflow_review_reason": "Motivo",
                    "workflow_attempt": "Intento",
                    "updated_at": "Actualizado",
                }
            )
            st.dataframe(queue_df, width="stretch", hide_index=True)
            st.caption(
                "La aprobación y la repetición de fases se gestionan en Revisión, "
                "donde están disponibles todas las opciones del flujo."
            )
            st.page_link(
                "pages/02_revision_manual.py",
                label="Abrir Revisión",
                icon=":material/fact_check:",
            )
        else:
            st.caption("No hay libros pendientes de revisión.")

    with st.expander("Detalle de estados", expanded=False):
        st.markdown("**Estados del flujo**")
        st.dataframe(
            pd.DataFrame([workflow_status_counts]),
            width="stretch",
            hide_index=True,
        )
        st.markdown("**Estados del formulario**")
        st.dataframe(
            pd.DataFrame([form_status_counts]),
            width="stretch",
            hide_index=True,
        )
        st.markdown("**Nodos en ejecución**")
        st.dataframe(pd.DataFrame([running_nodes]), width="stretch", hide_index=True)

if (
    last_job is not None
    and str(last_job.get("status") or "") not in ACTIVE_JOB_STATUSES
):
    last_status = str(last_job.get("status") or "")
    job_result = (
        last_job.get("result") if isinstance(last_job.get("result"), dict) else {}
    )
    processed = int(job_result.get("processed") or last_job.get("processed") or 0)
    requested = int(job_result.get("requested") or last_job.get("requested") or 0)
    if last_status == "completed":
        st.success(f"Última ejecución completada: {processed} de {requested}.")
    elif last_status == "stopped":
        st.warning(
            f"Última ejecución detenida de forma segura: {processed} de "
            f"{requested} elementos procesados."
        )
    elif last_status == "failed":
        st.error(
            "La última ejecución falló: "
            f"{last_job.get('error') or 'error desconocido'}"
        )

    with st.expander("Resultado de la última ejecución", expanded=False):
        result_items = list(job_result.get("items") or [])
        if result_items:
            st.dataframe(pd.DataFrame(result_items), width="stretch", hide_index=True)
        else:
            st.caption("La ejecución no produjo resultados individuales.")
        cover_sweep = job_result.get("cover_sweep")
        if isinstance(cover_sweep, dict):
            st.caption(
                "Barrido de portadas: "
                f"{int(cover_sweep.get('processed') or 0)} de "
                f"{int(cover_sweep.get('requested') or 0)}."
            )
            cover_items = list(cover_sweep.get("items") or [])
            if cover_items:
                st.dataframe(
                    pd.DataFrame(cover_items),
                    width="stretch",
                    hide_index=True,
                )

st.divider()

st.subheader("Ejecutar una fase o un tramo")
st.caption("Define el alcance; después ajusta los modelos y las opciones aplicables.")

col1, col2, col3 = st.columns([1.4, 1, 1])
with col1:
    selected_id = st.text_input(
        "ID del libro (opcional)", value="", placeholder="03B0001"
    )
with col2:
    start_stage_key = "orq_start_stage"
    if start_stage_key not in st.session_state:
        st.session_state[start_stage_key] = WORKFLOW_STAGES[0]
    start_stage = st.selectbox(
        "Desde",
        WORKFLOW_STAGES,
        key=start_stage_key,
        format_func=lambda value: STAGE_LABELS.get(str(value), str(value)),
    )
with col3:
    start_option_idx = STAGE_INDEX.get(start_stage, 0)
    stop_options = ["(sin límite)", *WORKFLOW_STAGES[start_option_idx:]]
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
    stop_after = st.selectbox(
        "Parar en",
        stop_options,
        key=stop_after_key,
        format_func=lambda value: (
            value
            if value == "(sin límite)"
            else STAGE_LABELS.get(str(value), str(value))
        ),
    )
    st.session_state[prev_start_key] = start_stage

option_col_a, option_col_b = st.columns(2)
with option_col_a:
    overwrite = st.checkbox(
        "Sobrescribir etapas ya completas",
        value=False,
        help=(
            "Permite repetir la etapa elegida en libros que ya han avanzado más. "
            "Los libros con el flujo completado, consolidados, en revisión o en "
            "ejecución siempre quedan excluidos."
        ),
    )
with option_col_b:
    max_attempts = st.number_input(
        "Reintentos máximos", min_value=0, max_value=20, value=2
    )
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
        f"No hay elementos elegibles para "
        f"'{STAGE_LABELS.get(start_stage, start_stage)}'{overwrite_text} "
        "en el módulo seleccionado. Si el rango incluye metadatos y mantienes "
        "activada la descarga automática, aún puedes ejecutar el barrido de "
        "portadas pendientes."
    )
    limit = 0
else:
    eligibility_text = (
        "incluyendo etapas posteriores" if overwrite else "en la etapa exacta"
    )
    st.caption(
        f"Elegibles para '{STAGE_LABELS.get(start_stage, start_stage)}' "
        f"({eligibility_text}): {eligible_limit}"
    )
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
        ocr_model = render_openai_ocr_model_selector(
            label="Modelo OCR",
            key="orq_ocr_model_openai",
            default_model=OCR_OPENAI_MODEL_DEFAULT,
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
        catalog_model = render_openai_catalog_model_selector(
            label="Modelo del catálogo",
            key="orq_catalog_model_openai",
            default_model=CATALOG_OPENAI_MODEL_DEFAULT,
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

selected_stage_names = [
    STAGE_LABELS[stage]
    for stage in WORKFLOW_STAGES
    if start_idx <= STAGE_INDEX[stage] <= stop_idx
]
target_text = (
    f"el libro {selected_id.strip()}"
    if selected_id.strip()
    else f"hasta {int(limit)} libros elegibles"
)
with st.container(border=True):
    st.markdown("**Resumen de la orden**")
    st.caption(
        f"{target_text} · {' → '.join(selected_stage_names)} · "
        f"{'con' if overwrite else 'sin'} sobrescritura · "
        f"{int(max_attempts)} reintentos máximos"
    )

if st.button(
    "Ejecutar flujo",
    type="primary",
    icon=":material/play_arrow:",
    disabled=active_job is not None,
    width="stretch",
):
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
        with st.spinner("Iniciando la ejecución…"):
            response = api_post("/workflow/jobs", json=payload, timeout=15.0)
        started_job = response.get("job") or {}
        started_job_id = str(started_job.get("job_id") or "").strip()
        if not started_job_id:
            raise RuntimeError("El backend no devolvió el identificador de ejecución.")
        st.session_state[LAST_JOB_SESSION_KEY] = started_job_id
        st.success(
            "Ejecución iniciada. Puedes actualizar el estado o detener el lote "
            "desde la parte superior."
        )
        st.rerun()
    except Exception as exc:
        st.error(f"No se pudo iniciar el flujo: {exc}")

st.divider()
with st.expander("Descarga manual de portadas", expanded=False):
    st.markdown("#### Descarga de portadas")
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
            width="stretch",
        )

    if download_covers:
        try:
            with st.spinner("Descargando las portadas pendientes…"):
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
                st.dataframe(
                    pd.DataFrame(cover_items), width="stretch", hide_index=True
                )
        except Exception as exc:
            st.error(f"Error descargando portadas: {exc}")
st.divider()
with st.expander("Información técnica del flujo", expanded=False):
    st.caption(
        "Definición interna del grafo de ejecución. Esta información sirve para "
        "diagnóstico y no es necesaria para operar el catálogo."
    )
    try:
        graph = api_get("/workflow/graph", timeout=12.0)
        st.write("LangGraph disponible:", graph.get("langgraph_available"))
        graph_col_a, graph_col_b = st.columns(2)
        with graph_col_a:
            st.markdown("**Nodos**")
            st.dataframe(
                pd.DataFrame(graph.get("nodes", [])),
                width="stretch",
                hide_index=True,
            )
        with graph_col_b:
            st.markdown("**Aristas**")
            st.dataframe(
                pd.DataFrame(graph.get("edges", [])),
                width="stretch",
                hide_index=True,
            )
    except Exception as exc:
        st.error(f"No se pudo cargar la definición del flujo: {exc}")
