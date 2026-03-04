import pandas as pd
import streamlit as st

try:
    from src.frontend.utils import (
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
        scope_params,
        select_book_id,
        select_module_scope,
        show_backend_status,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        WORKFLOW_STAGES,
        api_get,
        api_post,
        configure_page,
        load_ollama_models,
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

default_ollama_model = "glm-ocr:latest"
st.text_input("OCR provider", value="ollama", disabled=True)
ocr_provider = "ollama"
try:
    models = load_ollama_models()
except Exception:
    models = []
options = [default_ollama_model] + [name for name in models if name != default_ollama_model]
if len(options) > 1:
    ocr_model = st.selectbox("Modelo OCR", options, index=0)
else:
    ocr_model = st.text_input(
        "Modelo OCR",
        value=default_ollama_model,
        placeholder=default_ollama_model,
    )

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
        "ocr_provider": ocr_provider,
        "ocr_model": ocr_model.strip() or None,
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
