import base64
import hashlib
from datetime import datetime
from typing import Any

import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_get_bytes,
        api_post,
        configure_page,
        select_module_scope,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_get_bytes,
        api_post,
        configure_page,
        select_module_scope,
    )

configure_page("Datos | Media Catalog Books")
st.title("Fase 5 - Datos")


def _snapshot_origin(snapshot: dict[str, Any]) -> str:
    return f"{snapshot.get('source_actor') or ''}/{snapshot.get('source_device') or ''}".strip(
        "/"
    )


def _snapshot_label(snapshot_by_id: dict[str, dict[str, Any]], snapshot_id: str) -> str:
    snapshot = snapshot_by_id.get(snapshot_id) or {}
    parts = [
        str(snapshot.get("created_at") or "Sin fecha"),
        _snapshot_origin(snapshot) or "Origen desconocido",
        snapshot_id,
    ]
    return " | ".join(parts)


def _snapshot_rows(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        rows.append(
            {
                "Fecha": snapshot.get("created_at"),
                "ID": snapshot.get("snapshot_id"),
                "Origen": _snapshot_origin(snapshot),
                "Esquema": snapshot.get("schema_version"),
                "Tamaño (MB)": round(
                    float(snapshot.get("db_size_bytes") or 0) / (1024 * 1024), 2
                ),
                "Válido": bool(snapshot.get("valid")),
                "Importable": bool(snapshot.get("importable")),
                "Requiere migración": bool(snapshot.get("migration_required")),
                "Protegido": bool(snapshot.get("protected")),
                "Notas": snapshot.get("notes"),
                "Error": snapshot.get("error") or snapshot.get("compatibility_error"),
            }
        )
    return rows


st.subheader("Revisión externa en Excel")
with st.container(border=True):
    st.write(
        "Exporta las fichas de un módulo como archivo .xlsx, complétalas fuera de "
        "la aplicación y vuelve a importarlas. El archivo usa texto Unicode y "
        "conserva ISBN y referencias como texto."
    )
    st.caption(
        "La importación analiza primero todos los cambios. No modifica la base "
        "hasta que revises el informe y confirmes la operación."
    )

    review_block, review_module = select_module_scope(
        key_prefix="external_review_scope",
        title="Fichas que se incluirán en el Excel",
    )
    status_labels = {
        "Borradores": "draft",
        "Consolidadas": "consolidated",
        "Todas las fichas existentes": "all",
    }
    selected_status_label = st.selectbox(
        "Estado de las fichas",
        list(status_labels),
        key="external_review_status",
    )
    selected_status = status_labels[selected_status_label]

    if st.button(
        "Preparar Excel de revisión",
        type="primary",
        disabled=not review_module,
        key="external_review_export",
    ):
        try:
            workbook_bytes = api_get_bytes(
                "/core-books/review-workbook",
                params={
                    "block": review_block,
                    "module": review_module,
                    "form_status": selected_status,
                },
                timeout=LONG_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            st.error(f"No se pudo preparar el Excel: {exc}")
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.session_state["external_review_export_bytes"] = workbook_bytes
            st.session_state["external_review_export_name"] = (
                f"revision_{review_module}{review_block}_{selected_status}_"
                f"{timestamp}.xlsx"
            )
            st.success("Excel preparado. Ya puedes descargarlo.")

    export_bytes = st.session_state.get("external_review_export_bytes")
    export_name = st.session_state.get("external_review_export_name")
    if isinstance(export_bytes, (bytes, bytearray)) and export_name:
        st.download_button(
            "Descargar Excel de revisión",
            data=bytes(export_bytes),
            file_name=str(export_name),
            mime=(
                "application/vnd.openxmlformats-officedocument." "spreadsheetml.sheet"
            ),
            key="external_review_download",
        )

    st.divider()
    uploaded_workbook = st.file_uploader(
        "Excel de revisión completado",
        type="xlsx",
        max_upload_size=25,
        key="external_review_upload",
        help="Debe ser el mismo archivo generado por esta sección.",
    )

    if uploaded_workbook is not None:
        uploaded_bytes = uploaded_workbook.getvalue()
        uploaded_digest = hashlib.sha256(uploaded_bytes).hexdigest()
        if st.session_state.get("external_review_uploaded_digest") != uploaded_digest:
            st.session_state["external_review_uploaded_digest"] = uploaded_digest
            st.session_state.pop("external_review_preview", None)
            st.session_state.pop("external_review_import_bytes", None)

        if st.button(
            "Analizar cambios del Excel",
            key="external_review_preview_button",
        ):
            try:
                preview = api_post(
                    "/core-books/review-workbook/preview",
                    json={
                        "content_base64": base64.b64encode(uploaded_bytes).decode(
                            "ascii"
                        )
                    },
                    timeout=LONG_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                st.session_state.pop("external_review_preview", None)
                st.session_state.pop("external_review_import_bytes", None)
                st.error(f"No se pudo analizar el Excel: {exc}")
            else:
                st.session_state["external_review_preview"] = preview
                st.session_state["external_review_import_bytes"] = uploaded_bytes

    preview = st.session_state.get("external_review_preview")
    if isinstance(preview, dict):
        workbook_info = preview.get("workbook") or {}
        st.caption(
            "Plantilla: "
            f"{workbook_info.get('block')}/{workbook_info.get('module')} · "
            f"{preview.get('rows', 0)} fichas · "
            f"{workbook_info.get('format', 'OOXML (.xlsx)')}"
        )

        metric_books, metric_fields, metric_errors, metric_warnings = st.columns(4)
        metric_books.metric(
            "Fichas con cambios",
            int(preview.get("changed_books") or 0),
        )
        metric_fields.metric(
            "Campos que cambiarán",
            int(preview.get("changed_fields") or 0),
        )
        metric_errors.metric(
            "Errores",
            int(preview.get("errors_count") or 0),
        )
        metric_warnings.metric(
            "Avisos",
            int(preview.get("warnings_count") or 0),
        )

        errors = list(preview.get("errors") or [])
        warnings = list(preview.get("warnings") or [])
        changes = list(preview.get("changes") or [])

        if errors:
            st.error(
                "Hay errores que impiden importar. Corrígelos en el Excel y "
                "vuelve a analizarlo."
            )
            st.dataframe(errors, hide_index=True, width="stretch")
        if warnings:
            st.warning(
                "Estos avisos no bloquean la importación, pero conviene revisarlos."
            )
            st.dataframe(warnings, hide_index=True, width="stretch")
        if changes:
            st.write("Cambios que se aplicarán")
            st.dataframe(changes, hide_index=True, width="stretch")
        elif not errors:
            st.info("El Excel no contiene cambios respecto a la exportación.")

        can_apply = bool(preview.get("can_apply"))
        consolidate_after_import = st.checkbox(
            "Consolidar después de importar las fichas modificadas que estén en borrador",
            value=False,
            disabled=not can_apply,
            key="external_review_consolidate",
        )
        st.caption(
            "Si no marcas esta opción, cada ficha conserva su estado actual. "
            "Las fichas consolidadas pueden corregirse mediante esta importación "
            "explícita y continúan consolidadas."
        )
        confirm_import = st.checkbox(
            "He revisado la lista completa y confirmo estos cambios",
            disabled=not can_apply,
            key="external_review_confirm",
        )
        if st.button(
            "Aplicar cambios del Excel",
            type="primary",
            disabled=not can_apply or not confirm_import,
            key="external_review_apply",
        ):
            import_bytes = st.session_state.get("external_review_import_bytes")
            if not isinstance(import_bytes, (bytes, bytearray)):
                st.error("Vuelve a analizar el archivo antes de importarlo.")
            else:
                try:
                    result = api_post(
                        "/core-books/review-workbook/apply",
                        json={
                            "content_base64": base64.b64encode(
                                bytes(import_bytes)
                            ).decode("ascii"),
                            "workbook_sha256": preview.get("workbook_sha256"),
                            "confirm": True,
                            "consolidate_after_import": bool(consolidate_after_import),
                        },
                        timeout=LONG_TIMEOUT_SECONDS,
                    )
                except Exception as exc:
                    st.error(f"No se pudieron aplicar los cambios: {exc}")
                else:
                    if result.get("applied"):
                        st.success(
                            "Importación completada: "
                            f"{int(result.get('applied_books') or 0)} fichas "
                            "actualizadas y descripciones regeneradas."
                        )
                        st.session_state["external_review_preview"] = result
                    else:
                        st.error(
                            "La importación no se aplicó porque la validación "
                            "detectó cambios o errores nuevos."
                        )
                        st.session_state["external_review_preview"] = result

st.divider()
st.subheader("Instantáneas de la base de datos")


try:
    status = api_get("/snapshots/status", timeout=LONG_TIMEOUT_SECONDS)
    snapshots_payload = api_get("/snapshots", timeout=LONG_TIMEOUT_SECONDS)
except Exception as exc:
    st.error(f"No se pudo cargar el estado de datos: {exc}")
    st.stop()

snapshots = list(snapshots_payload.get("snapshots") or [])
importable_snapshots = [
    snapshot for snapshot in snapshots if snapshot.get("importable")
]
snapshot_by_id = {
    str(snapshot.get("snapshot_id")): snapshot
    for snapshot in importable_snapshots
    if snapshot.get("snapshot_id")
}
external_snapshot = status.get("latest_external_snapshot") or None
external_snapshot_id = (
    str(external_snapshot.get("snapshot_id")) if external_snapshot else None
)

summary_left, summary_middle, summary_right = st.columns(3, gap="large")
summary_left.metric("Instantáneas", int(status.get("snapshots_count") or 0))
summary_middle.metric("Importables", int(status.get("importable_snapshots_count") or 0))
summary_right.metric(
    "Incompatibles", int(status.get("incompatible_snapshots_count") or 0)
)

st.caption(f"Base local: `{status.get('local_db_path')}`")
st.caption(f"Carpeta de instantáneas: `{status.get('snapshots_dir')}`")
st.caption(f"Origen: `{status.get('actor')}` / `{status.get('device')}`")
st.caption(f"Esquema local: `{status.get('schema_version')}`")

if external_snapshot:
    st.warning(
        "Instantánea externa pendiente de importar: "
        f"`{external_snapshot_id}` ({_snapshot_origin(external_snapshot) or 'origen desconocido'})."
    )
else:
    st.caption("No se detectan instantáneas externas pendientes de importar.")

action_left, action_right = st.columns([1, 1], gap="small")
with action_left:
    if st.button("Publicar instantánea", type="primary", width="stretch"):
        try:
            result = api_post(
                "/snapshots/publish",
                json={"notes": "Instantánea manual desde Streamlit"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            st.error(f"No se pudo publicar la instantánea: {exc}")
        else:
            snapshot = result.get("snapshot") or {}
            st.success(f"Instantánea publicada: `{snapshot.get('snapshot_id')}`")
            st.rerun()

with action_right:
    if st.button("Limpiar instantáneas antiguas", width="stretch"):
        try:
            result = api_post("/snapshots/cleanup", timeout=LONG_TIMEOUT_SECONDS)
        except Exception as exc:
            st.error(f"No se pudieron limpiar las instantáneas: {exc}")
        else:
            st.success(f"Instantáneas eliminadas: {len(result.get('deleted') or [])}")
            st.rerun()

with st.expander("Importar instantánea", expanded=bool(external_snapshot)):
    st.caption(
        "La importación sustituye la base local por la instantánea elegida. "
        "Primero se valida y migra una copia; después se crea una copia de seguridad "
        "automática en `data/backups/local`."
    )
    if not importable_snapshots:
        st.info("No hay instantáneas válidas y compatibles para importar.")
    else:
        snapshot_ids = list(snapshot_by_id)
        default_index = (
            snapshot_ids.index(external_snapshot_id)
            if external_snapshot_id in snapshot_ids
            else 0
        )
        selected_snapshot_id = st.selectbox(
            "Instantánea",
            snapshot_ids,
            index=default_index,
            format_func=lambda snapshot_id: _snapshot_label(
                snapshot_by_id, snapshot_id
            ),
        )
        selected_snapshot = snapshot_by_id[selected_snapshot_id]
        if selected_snapshot.get("migration_required"):
            st.info(
                f"El esquema `{selected_snapshot.get('schema_version')}` se "
                f"actualizará a `{selected_snapshot.get('current_schema_version')}` "
                "antes de sustituir la base local."
            )
        else:
            st.caption(
                f"Esquema compatible: `{selected_snapshot.get('schema_version')}`."
            )
        confirm_import = st.checkbox(
            "Confirmo que quiero sustituir la base local por la instantánea seleccionada"
        )
        if st.button(
            "Importar instantánea seleccionada",
            type="primary",
            disabled=not confirm_import,
            width="stretch",
        ):
            try:
                result = api_post(
                    "/snapshots/import",
                    json={"snapshot_id": selected_snapshot_id, "confirm": True},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                st.error(f"No se pudo importar la instantánea: {exc}")
            else:
                imported_snapshot = result.get("snapshot") or {}
                st.success(
                    f"Instantánea importada: `{imported_snapshot.get('snapshot_id')}`"
                )
                if result.get("backup_path"):
                    st.info(
                        f"Copia de seguridad local creada: `{result.get('backup_path')}`"
                    )
                migration = result.get("migration") or {}
                applied_now = migration.get("applied_now") or []
                if applied_now:
                    st.info(
                        "Migraciones aplicadas a la copia importada: "
                        + ", ".join(f"`{version}`" for version in applied_now)
                    )
                st.caption(f"Esquema importado: `{migration.get('schema_version')}`.")
                st.warning(
                    "Reinicia la aplicación antes de continuar para cerrar "
                    "cualquier operación que estuviera usando la base anterior."
                )

rows = _snapshot_rows(snapshots)
if rows:
    st.dataframe(rows, hide_index=True, width="stretch")
else:
    st.info("Aún no hay instantáneas publicadas.")
