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
        load_ollama_models,
        render_ollama_model_selector,
        seed_widget_once,
        scope_params,
        select_book_id,
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
        load_ollama_models,
        render_ollama_model_selector,
        seed_widget_once,
        scope_params,
        select_book_id,
        select_module_scope,
        show_backend_status,
    )

configure_page("Ejecucion por etapa | Media Catalog Books")

st.title("Fase 3 · Ejecucion por etapa")
show_backend_status()

scope_block, scope_module = select_module_scope(key_prefix="stage_scope", title="Modulo de trabajo")
if not scope_module:
    st.stop()

rows = api_get("/books", params={"limit": 1000, **scope_params(scope_block, scope_module)}, timeout=20.0)
if not rows:
    st.info("No hay libros cargados en el modulo seleccionado. Ejecuta la ingesta primero.")
    st.stop()

selected_id = select_book_id(rows, label="Libro", key="stage_runner_book")

col_a, col_b, col_c = st.columns(3)
with col_a:
    stage = st.selectbox("Etapa", WORKFLOW_STAGES)
with col_b:
    overwrite = st.checkbox("Sobrescribir", value=False)
with col_c:
    max_attempts = st.number_input("Reintentos", min_value=0, max_value=20, value=2)

try:
    models = load_ollama_models()
except Exception:
    models = []

ocr_controls_enabled = stage == "ocr"
catalog_controls_enabled = stage == "catalog"

ocr_provider_options = ["openai", "ollama"]
ocr_provider_index = 0
if OCR_PROVIDER_DEFAULT in ocr_provider_options:
    ocr_provider_index = ocr_provider_options.index(OCR_PROVIDER_DEFAULT)
seed_widget_once("stage_ocr_provider", ocr_provider_options[ocr_provider_index])
ocr_provider = st.selectbox(
    "OCR provider",
    ocr_provider_options,
    key="stage_ocr_provider",
    disabled=not ocr_controls_enabled,
)
if ocr_provider == "ollama":
    ocr_model = render_ollama_model_selector(
        label="Modelo OCR",
        key="stage_ocr_model_ollama",
        installed_models=models,
        default_model=OCR_OLLAMA_MODEL_DEFAULT,
        suggested_models=OCR_OLLAMA_MODEL_SUGGESTIONS,
        disabled=not ocr_controls_enabled,
    )
else:
    seed_widget_once("stage_ocr_model_openai", OCR_OPENAI_MODEL_DEFAULT)
    ocr_model = st.text_input(
        "Modelo OCR",
        placeholder=OCR_OPENAI_MODEL_DEFAULT,
        key="stage_ocr_model_openai",
        disabled=not ocr_controls_enabled,
    )
seed_widget_once("stage_ocr_resize_to_1800", OCR_RESIZE_TO_1800_DEFAULT)
ocr_resize_to_1800 = st.checkbox(
    "Reducir imagen a 1800 px (solo para glm-ocr)",
    key="stage_ocr_resize_to_1800",
    help="Si esta activo y el modelo OCR empieza por 'glm-ocr', se redimensiona la imagen al lado maximo 1800.",
    disabled=not ocr_controls_enabled,
)
if not ocr_controls_enabled:
    st.caption("Controles OCR desactivados: la etapa seleccionada no es OCR.")

st.caption("Configuracion de catalogacion automatica")
catalog_col_a, catalog_col_b = st.columns([1, 2])
with catalog_col_a:
    catalog_provider_options = ["openai", "ollama"]
    catalog_provider_index = 0
    if CATALOG_PROVIDER_DEFAULT in catalog_provider_options:
        catalog_provider_index = catalog_provider_options.index(CATALOG_PROVIDER_DEFAULT)
    seed_widget_once("stage_catalog_provider", catalog_provider_options[catalog_provider_index])
    catalog_provider = st.selectbox(
        "Provider catalogo",
        catalog_provider_options,
        key="stage_catalog_provider",
        disabled=not catalog_controls_enabled,
    )
with catalog_col_b:
    if catalog_provider == "ollama":
        catalog_model = render_ollama_model_selector(
            label="Modelo catalogo",
            key="stage_catalog_model_ollama",
            installed_models=models,
            default_model=CATALOG_OLLAMA_MODEL_DEFAULT,
            suggested_models=CATALOG_OLLAMA_MODEL_SUGGESTIONS,
            disabled=not catalog_controls_enabled,
        )
    else:
        seed_widget_once("stage_catalog_model_openai", CATALOG_OPENAI_MODEL_DEFAULT)
        catalog_model = st.text_input(
            "Modelo catalogo",
            placeholder=CATALOG_OPENAI_MODEL_DEFAULT,
            key="stage_catalog_model_openai",
            disabled=not catalog_controls_enabled,
        )
if not catalog_controls_enabled:
    st.caption("Controles de catalogacion desactivados: la etapa seleccionada no es catalog.")

if ocr_controls_enabled:
    st.caption(f"OCR efectivo (si no tocas nada): `{ocr_provider}` / `{ocr_model}`")
if catalog_controls_enabled:
    st.caption(f"Catalog efectivo (si no tocas nada): `{catalog_provider}` / `{catalog_model}`")

if st.button("Ejecutar etapa", type="primary"):
    payload = {
        "book_id": selected_id,
        "block": scope_block,
        "module": scope_module,
        "limit": 1,
        "start_stage": stage,
        "stop_after": stage,
        "overwrite": overwrite,
        "max_attempts": int(max_attempts),
        "ocr_provider": ocr_provider if stage == "ocr" else None,
        "ocr_model": (ocr_model.strip() or None) if stage == "ocr" else None,
        "ocr_resize_to_1800": bool(ocr_resize_to_1800) if stage == "ocr" else False,
        "catalog_provider": catalog_provider if stage == "catalog" else None,
        "catalog_model": (catalog_model.strip() or None) if stage == "catalog" else None,
    }
    try:
        result = api_post("/workflow/run", json=payload, timeout=600.0)
        st.success("Etapa ejecutada")
        st.dataframe(pd.DataFrame(result.get("items", [])), width="stretch", hide_index=True)
    except Exception as exc:
        st.error(f"Error ejecutando etapa: {exc}")

st.subheader("Detalle actual del libro")
try:
    current = api_get(f"/books/{selected_id}", timeout=20.0)
    st.json(
        {
            "id": current.get("id"),
            "block": current.get("block"),
            "module": current.get("module"),
            "pipeline_stage": current.get("pipeline_stage"),
            "workflow_status": current.get("workflow_status"),
            "ocr_status": current.get("ocr_status"),
            "metadata_status": current.get("metadata_status"),
            "catalog_status": current.get("catalog_status"),
            "cover_status": current.get("cover_status"),
            "cover_path": current.get("cover_path"),
        }
    )

    with st.expander("Metadata JSON", expanded=False):
        st.json(current.get("metadata") or {})

    with st.expander("Catalog JSON", expanded=False):
        st.json(current.get("catalog") or {})

    catalog_payload = current.get("catalog") if isinstance(current.get("catalog"), dict) else {}
    qa = catalog_payload.get("qa") if isinstance(catalog_payload.get("qa"), dict) else {}
    if qa:
        st.subheader("QA catalogo")
        c1, c2, c3 = st.columns(3)
        c1.metric("Confidence", qa.get("confidence", "-"))
        c2.metric("Requires review", "si" if qa.get("requires_manual_review") else "no")
        conflicts = qa.get("conflicts") if isinstance(qa.get("conflicts"), dict) else {}
        c3.metric("Conflicts", len(conflicts))

        flags = qa.get("review_flags") if isinstance(qa.get("review_flags"), list) else []
        if flags:
            st.warning("Flags QA: " + ", ".join(str(item) for item in flags[:10]))
        if conflicts:
            st.error("Conflictos QA detectados")
            st.json(conflicts)
except Exception as exc:
    st.error(f"No se pudo cargar el detalle: {exc}")
