from typing import Any

import streamlit as st

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        configure_page,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import LONG_TIMEOUT_SECONDS, api_get, api_post, configure_page

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
                "Tamano MB": round(
                    float(snapshot.get("db_size_bytes") or 0) / (1024 * 1024), 2
                ),
                "Valido": bool(snapshot.get("valid")),
                "Importable": bool(snapshot.get("importable")),
                "Requiere migracion": bool(snapshot.get("migration_required")),
                "Protegido": bool(snapshot.get("protected")),
                "Notas": snapshot.get("notes"),
                "Error": snapshot.get("error") or snapshot.get("compatibility_error"),
            }
        )
    return rows


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
summary_left.metric("Snapshots", int(status.get("snapshots_count") or 0))
summary_middle.metric("Importables", int(status.get("importable_snapshots_count") or 0))
summary_right.metric(
    "Incompatibles", int(status.get("incompatible_snapshots_count") or 0)
)

st.caption(f"Base local: `{status.get('local_db_path')}`")
st.caption(f"Carpeta de snapshots: `{status.get('snapshots_dir')}`")
st.caption(f"Origen: `{status.get('actor')}` / `{status.get('device')}`")
st.caption(f"Esquema local: `{status.get('schema_version')}`")

if external_snapshot:
    st.warning(
        "Snapshot externo pendiente de importar: "
        f"`{external_snapshot_id}` ({_snapshot_origin(external_snapshot) or 'origen desconocido'})."
    )
else:
    st.caption("No se detectan snapshots externos pendientes de importar.")

action_left, action_right = st.columns([1, 1], gap="small")
with action_left:
    if st.button("Publicar snapshot", type="primary", width="stretch"):
        try:
            result = api_post(
                "/snapshots/publish",
                json={"notes": "Snapshot manual desde Streamlit"},
                timeout=LONG_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            st.error(f"No se pudo publicar el snapshot: {exc}")
        else:
            snapshot = result.get("snapshot") or {}
            st.success(f"Snapshot publicado: `{snapshot.get('snapshot_id')}`")
            st.rerun()

with action_right:
    if st.button("Limpiar snapshots antiguos", width="stretch"):
        try:
            result = api_post("/snapshots/cleanup", timeout=LONG_TIMEOUT_SECONDS)
        except Exception as exc:
            st.error(f"No se pudieron limpiar los snapshots: {exc}")
        else:
            st.success(f"Snapshots eliminados: {len(result.get('deleted') or [])}")
            st.rerun()

with st.expander("Importar snapshot", expanded=bool(external_snapshot)):
    st.caption(
        "La importacion sustituye la base local por el snapshot elegido. "
        "Primero se valida y migra una copia; despues se crea un backup "
        "automatico en `data/backups/local`."
    )
    if not importable_snapshots:
        st.info("No hay snapshots validos y compatibles para importar.")
    else:
        snapshot_ids = list(snapshot_by_id)
        default_index = (
            snapshot_ids.index(external_snapshot_id)
            if external_snapshot_id in snapshot_ids
            else 0
        )
        selected_snapshot_id = st.selectbox(
            "Snapshot",
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
                f"actualizara a `{selected_snapshot.get('current_schema_version')}` "
                "antes de sustituir la base local."
            )
        else:
            st.caption(
                f"Esquema compatible: `{selected_snapshot.get('schema_version')}`."
            )
        confirm_import = st.checkbox(
            "Confirmo que quiero sustituir la base local por el snapshot seleccionado"
        )
        if st.button(
            "Importar snapshot seleccionado",
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
                st.error(f"No se pudo importar el snapshot: {exc}")
            else:
                imported_snapshot = result.get("snapshot") or {}
                st.success(
                    f"Snapshot importado: `{imported_snapshot.get('snapshot_id')}`"
                )
                if result.get("backup_path"):
                    st.info(f"Backup local creado: `{result.get('backup_path')}`")
                migration = result.get("migration") or {}
                applied_now = migration.get("applied_now") or []
                if applied_now:
                    st.info(
                        "Migraciones aplicadas a la copia importada: "
                        + ", ".join(f"`{version}`" for version in applied_now)
                    )
                st.caption(f"Esquema importado: `{migration.get('schema_version')}`.")
                st.warning(
                    "Reinicia la aplicacion antes de continuar para cerrar "
                    "cualquier operacion que estuviera usando la base anterior."
                )

rows = _snapshot_rows(snapshots)
if rows:
    st.dataframe(rows, hide_index=True, width="stretch")
else:
    st.info("Aun no hay snapshots publicados.")
