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

scope_block, scope_module = select_module_scope(
    key_prefix="ingesta_scope", title="Módulo de trabajo"
)
if not scope_module:
    st.stop()

stats = load_stats(block=scope_block, module=scope_module)
col_a, col_b, col_c = st.columns(3)
col_a.metric("Total", stats.get("total", 0))
col_b.metric("Pend. OCR", stats.get("needs_ocr", 0))
col_c.metric("En revisión", stats.get("needs_workflow_review", 0))

st.info("La carpeta de entrada debe respetar la estructura: data/input/A|B|C/01..99")

default_folder = os.getenv("COVERS_DIR", "data/input")
plan_state_key = "ingest_preparation_plan"
result_state_key = "ingest_last_result"

stored_plan = st.session_state.get(plan_state_key)
if stored_plan and (
    stored_plan["payload"].get("block") != scope_block
    or stored_plan["payload"].get("module") != scope_module
):
    st.session_state.pop(plan_state_key, None)
    stored_plan = None

with st.form("ingest_form"):
    folder = st.text_input(
        "Carpeta de imágenes", value=default_folder, key="ingest_folder"
    )
    recursive = st.checkbox(
        "Buscar en subcarpetas del módulo", value=True, key="ingest_recursive"
    )
    overwrite_paths = st.checkbox(
        "Sobrescribir rutas ya registradas", value=False, key="ingest_overwrite"
    )
    normalize_names = st.checkbox(
        "Normalizar automáticamente los nombres reconocibles",
        value=True,
        key="ingest_normalize_names",
        help="Por ejemplo, 1a1_02.JPG pasa a 01A0001_2.jpg.",
    )
    convert_heic = st.checkbox(
        "Convertir HEIC/HEIF a JPEG",
        value=True,
        key="ingest_convert_heic",
    )
    delete_original_heic = st.checkbox(
        "Borrar el HEIC/HEIF original después de validar el JPEG",
        value=False,
        key="ingest_delete_heic",
        help="El original solo se borra cuando existe un JPEG válido.",
    )
    ext_text = st.text_input(
        "Extensiones (separadas por comas)",
        value="jpg,jpeg,png,webp,heic,heif",
        key="ingest_extensions",
    )

    submitted = st.form_submit_button(
        "Analizar módulo", icon=":material/preview:", type="primary"
    )

if submitted:
    extensions = [item.strip() for item in ext_text.split(",") if item.strip()]
    payload = {
        "folder": folder,
        "block": scope_block,
        "module": scope_module,
        "recursive": recursive,
        "extensions": extensions,
        "overwrite_existing_paths": overwrite_paths,
        "normalize_image_names": normalize_names,
        "convert_heic": convert_heic,
        "delete_original_heic": bool(delete_original_heic and convert_heic),
    }
    try:
        plan = api_post("/covers/ingest/plan", json=payload, timeout=900.0)
        st.session_state[plan_state_key] = {"payload": payload, "plan": plan}
        st.session_state.pop(result_state_key, None)
        st.session_state.pop("ingest_confirmed", None)
        stored_plan = st.session_state[plan_state_key]
    except Exception as exc:
        st.session_state.pop(plan_state_key, None)
        stored_plan = None
        st.error(f"No se pudo analizar el módulo: {exc}")

if stored_plan:
    plan = stored_plan["plan"]
    summary = plan.get("summary", {})
    st.subheader("Cambios propuestos")
    metric_a, metric_b, metric_c, metric_d = st.columns(4)
    metric_a.metric("Renombrados", summary.get("renames", 0))
    metric_b.metric("Conversiones", summary.get("conversions", 0))
    metric_c.metric("Borrados", summary.get("deletions", 0))
    metric_d.metric("Correcciones manuales", summary.get("manual_corrections", 0))
    st.caption(
        f"Quedarán listas para indexar {summary.get('files_ready', 0)} imágenes "
        f"de {summary.get('books_ready', 0)} libros."
    )

    action_labels = {
        "rename": "Renombrar",
        "convert_heic": "Convertir a JPEG",
        "delete_original": "Borrar original",
    }
    actions = [
        {
            "Cambio": action_labels.get(action.get("type"), action.get("type")),
            "Origen": action.get("source"),
            "Destino o copia validada": action.get("destination"),
        }
        for action in plan.get("actions", [])
    ]
    if actions:
        st.dataframe(pd.DataFrame(actions), width="stretch", hide_index=True)
    else:
        st.info("No hay cambios de archivos que aplicar.")

    issues = plan.get("issues", [])
    if issues:
        st.warning(
            "Estas incidencias requieren corrección manual. Las preparaciones seguras "
            "que figuren arriba pueden aplicarse, pero los libros afectados no se "
            "indexarán hasta que la secuencia, el nombre o la ubicación sean válidos."
        )
        st.dataframe(
            pd.DataFrame(
                [
                    {
                        "Archivo": issue.get("file"),
                        "Incidencia": issue.get("message"),
                    }
                    for issue in issues
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    confirmed = st.checkbox(
        "He revisado la lista y confirmo estos cambios",
        key="ingest_confirmed",
    )
    apply_label = "Aplicar cambios e indexar" if actions else "Indexar módulo"
    if st.button(
        apply_label,
        icon=":material/check_circle:",
        type="primary",
        key="apply_ingest_plan",
    ):
        if not confirmed:
            st.error("Revisa la lista y marca la confirmación antes de continuar.")
        else:
            confirmed_payload = dict(stored_plan["payload"])
            confirmed_payload["preparation_fingerprint"] = plan["fingerprint"]
            try:
                result = api_post(
                    "/covers/ingest", json=confirmed_payload, timeout=900.0
                )
                st.session_state[result_state_key] = result
                st.session_state.pop(plan_state_key, None)
                st.rerun()
            except Exception as exc:
                st.error(f"No se pudieron aplicar los cambios: {exc}")

last_result = st.session_state.pop(result_state_key, None)
if last_result:
    preparation = last_result.get("preparation", {})
    if preparation.get("failed"):
        st.error(f"La extracción terminó con {preparation['failed']} cambios fallidos.")
    else:
        st.success(
            f"Extracción completada: {last_result.get('books_detected', 0)} libros "
            f"y {last_result.get('images_indexed', 0)} imágenes indexadas."
        )
    if preparation.get("issues"):
        st.warning(
            f"Quedan {len(preparation['issues'])} incidencias para corregir a mano. "
            "Los libros afectados no se han indexado."
        )
    with st.expander("Ver informe completo"):
        st.json(last_result)

st.subheader("Vista rápida de libros")

stage_filter = st.selectbox(
    "Filtrar por etapa",
    [
        "(todas)",
        "ocr",
        "metadata",
        "catalog",
        "cover",
        "review",
        "done",
        "needs_workflow_review",
    ],
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
