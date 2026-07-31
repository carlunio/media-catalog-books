from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from src.project_meta import get_app_meta

from ..config import (
    BBDD_DIR,
    CLOUD_SNAPSHOTS_DIR,
    DB_PATH,
    SYNC_ACTOR,
    SYNC_DEVICE,
    SYNC_KEEP_MIN,
    SYNC_RETENTION_DAYS,
    SYNC_STATE_PATH,
)

SNAPSHOTS_SUBDIR = "snapshots"
SCHEMA_VERSION = "1"


class SnapshotError(RuntimeError):
    pass


def _snapshots_dir() -> Path:
    return CLOUD_SNAPSHOTS_DIR / SNAPSHOTS_SUBDIR


def _slug(value: str, fallback: str) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-")
    return text or fallback


def _now() -> datetime:
    return datetime.now().astimezone()


def _iso_now() -> str:
    return _now().isoformat(timespec="seconds")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.astimezone()
    return parsed


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _duckdb_sql_string(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def _repack_database(source_path: Path, target_path: Path) -> None:
    if target_path.exists():
        target_path.unlink()

    with duckdb.connect(str(source_path)) as con:
        con.execute("CHECKPOINT")
        db_list = con.execute("PRAGMA database_list").fetchall()
        if not db_list:
            raise SnapshotError("No se pudo resolver el catalogo activo de DuckDB.")
        catalog_name = str(db_list[0][1])
        con.execute(f"ATTACH {_duckdb_sql_string(target_path)} AS snapshot")
        con.execute(f'COPY FROM DATABASE "{catalog_name}" TO snapshot')
        con.execute("DETACH snapshot")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _update_sync_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    state = _read_json(SYNC_STATE_PATH) or {}
    now = _iso_now()
    state.update(
        {
            "last_published_snapshot_id": snapshot["snapshot_id"],
            "last_published_sha256": snapshot["sha256"],
            "last_sync_at": now,
        }
    )
    _write_json_atomic(SYNC_STATE_PATH, state)
    return state


def _update_import_state(snapshot: dict[str, Any], backup_path: Path | None) -> dict[str, Any]:
    state = _read_json(SYNC_STATE_PATH) or {}
    now = _iso_now()
    state.update(
        {
            "last_imported_snapshot_id": snapshot["snapshot_id"],
            "last_imported_sha256": snapshot.get("sha256") or snapshot.get("actual_sha256"),
            "last_imported_at": now,
            "last_sync_at": now,
            "last_import_backup_path": str(backup_path) if backup_path else None,
        }
    )
    _write_json_atomic(SYNC_STATE_PATH, state)
    return state


def _manifest_to_snapshot(manifest_path: Path, *, verify_hash: bool = True) -> dict[str, Any]:
    manifest = _read_json(manifest_path)
    if manifest is None:
        return {
            "snapshot_id": manifest_path.stem,
            "manifest_path": str(manifest_path),
            "valid": False,
            "error": "Manifiesto JSON no valido.",
        }

    db_filename = str(manifest.get("db_filename") or "")
    db_path = manifest_path.parent / db_filename
    snapshot = dict(manifest)
    snapshot["manifest_path"] = str(manifest_path)
    snapshot["path"] = str(db_path)
    snapshot["valid"] = True
    snapshot["error"] = None

    if not db_filename:
        snapshot["valid"] = False
        snapshot["error"] = "El manifiesto no indica db_filename."
        return snapshot
    if not db_path.exists():
        snapshot["valid"] = False
        snapshot["error"] = "El fichero DuckDB del snapshot no existe."
        return snapshot

    if verify_hash:
        expected_sha = str(manifest.get("sha256") or "")
        actual_sha = _sha256_file(db_path)
        snapshot["actual_sha256"] = actual_sha
        if not expected_sha:
            snapshot["valid"] = False
            snapshot["error"] = "El manifiesto no indica sha256."
        elif expected_sha != actual_sha:
            snapshot["valid"] = False
            snapshot["error"] = "El hash sha256 no coincide."

    created_at = _parse_datetime(snapshot.get("created_at"))
    snapshot["_created_at_sort"] = created_at.timestamp() if created_at else 0
    return snapshot


def _snapshot_created_at(snapshot: dict[str, Any]) -> datetime | None:
    return _parse_datetime(snapshot.get("created_at"))


def _known_snapshot_ids(state: dict[str, Any]) -> set[str]:
    keys = ("last_published_snapshot_id", "last_imported_snapshot_id")
    return {str(state.get(key)) for key in keys if state.get(key)}


def _is_own_snapshot(snapshot: dict[str, Any]) -> bool:
    return (
        str(snapshot.get("source_actor") or "") == SYNC_ACTOR
        and str(snapshot.get("source_device") or "") == SYNC_DEVICE
    )


def list_snapshots(*, verify_hash: bool = True, include_invalid: bool = True) -> list[dict[str, Any]]:
    snapshots_path = _snapshots_dir()
    if not snapshots_path.exists():
        return []

    snapshots = [
        _manifest_to_snapshot(path, verify_hash=verify_hash)
        for path in snapshots_path.glob("*.json")
    ]
    if not include_invalid:
        snapshots = [snapshot for snapshot in snapshots if snapshot.get("valid")]
    snapshots.sort(key=lambda item: float(item.get("_created_at_sort") or 0), reverse=True)
    for snapshot in snapshots:
        snapshot.pop("_created_at_sort", None)
    return snapshots


def detect_external_snapshot(
    snapshots: list[dict[str, Any]] | None = None,
    *,
    state: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    snapshot_list = snapshots if snapshots is not None else list_snapshots()
    valid_snapshots = [snapshot for snapshot in snapshot_list if snapshot.get("valid")]
    sync_state = state if state is not None else (_read_json(SYNC_STATE_PATH) or {})
    known_ids = _known_snapshot_ids(sync_state)

    known_dates = [
        created_at
        for snapshot in valid_snapshots
        if str(snapshot.get("snapshot_id")) in known_ids
        for created_at in [_snapshot_created_at(snapshot)]
        if created_at is not None
    ]
    latest_known_created_at = max(known_dates) if known_dates else None

    for snapshot in valid_snapshots:
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        if not snapshot_id or snapshot_id in known_ids or _is_own_snapshot(snapshot):
            continue

        created_at = _snapshot_created_at(snapshot)
        if latest_known_created_at and created_at and created_at <= latest_known_created_at:
            continue
        if latest_known_created_at and created_at is None:
            continue
        return snapshot

    return None


def get_status() -> dict[str, Any]:
    snapshots = list_snapshots(verify_hash=True, include_invalid=True)
    valid_snapshots = [snapshot for snapshot in snapshots if snapshot.get("valid")]
    sync_state = _read_json(SYNC_STATE_PATH) or {}
    latest_external_snapshot = detect_external_snapshot(snapshots, state=sync_state)
    return {
        "ok": True,
        "local_db_path": str(DB_PATH),
        "local_db_exists": DB_PATH.exists(),
        "bbdd_root": str(BBDD_DIR),
        "cloud_root": str(CLOUD_SNAPSHOTS_DIR),
        "snapshots_dir": str(_snapshots_dir()),
        "snapshots_dir_exists": _snapshots_dir().exists(),
        "sync_state_path": str(SYNC_STATE_PATH),
        "sync_state": sync_state,
        "actor": SYNC_ACTOR,
        "device": SYNC_DEVICE,
        "retention_days": SYNC_RETENTION_DAYS,
        "keep_min": SYNC_KEEP_MIN,
        "snapshots_count": len(valid_snapshots),
        "latest_snapshot": valid_snapshots[0] if valid_snapshots else None,
        "latest_external_snapshot": latest_external_snapshot,
        "has_external_snapshot": latest_external_snapshot is not None,
    }


def _new_snapshot_id() -> str:
    timestamp = _now().strftime("%Y%m%d_%H%M%S_%f")
    actor = _slug(SYNC_ACTOR, "usuario")
    device = _slug(SYNC_DEVICE, "equipo")
    return f"{timestamp}_{actor}_{device}"


def publish_snapshot(*, notes: str | None = None, cleanup: bool = True) -> dict[str, Any]:
    if not DB_PATH.exists():
        raise SnapshotError(f"No existe la base local: {DB_PATH}")

    snapshots_path = _snapshots_dir()
    snapshots_path.mkdir(parents=True, exist_ok=True)

    snapshot_id = _new_snapshot_id()
    final_db_path = snapshots_path / f"{snapshot_id}.duckdb"
    tmp_db_path = snapshots_path / f"{snapshot_id}.tmp.duckdb"
    manifest_path = snapshots_path / f"{snapshot_id}.json"

    if final_db_path.exists() or manifest_path.exists():
        raise SnapshotError(f"Ya existe un snapshot con id {snapshot_id}.")

    try:
        _repack_database(DB_PATH, tmp_db_path)
        db_size = tmp_db_path.stat().st_size
        file_sha256 = _sha256_file(tmp_db_path)
        tmp_db_path.replace(final_db_path)

        app_meta = get_app_meta()
        manifest = {
            "snapshot_id": snapshot_id,
            "created_at": _iso_now(),
            "app_version": app_meta.version,
            "schema_version": SCHEMA_VERSION,
            "source_actor": SYNC_ACTOR,
            "source_device": SYNC_DEVICE,
            "source_db_path": str(DB_PATH),
            "db_filename": final_db_path.name,
            "db_size_bytes": db_size,
            "sha256": file_sha256,
            "protected": False,
            "notes": str(notes or "Snapshot manual").strip() or "Snapshot manual",
        }
        _write_json_atomic(manifest_path, manifest)
        state = _update_sync_state(manifest)
        cleanup_result = cleanup_snapshots() if cleanup else {"deleted": [], "kept": []}
        snapshot = _manifest_to_snapshot(manifest_path, verify_hash=False)
        return {
            "ok": True,
            "snapshot": snapshot,
            "sync_state": state,
            "cleanup": cleanup_result,
        }
    finally:
        if tmp_db_path.exists():
            tmp_db_path.unlink()


def _find_snapshot(snapshot_id: str) -> dict[str, Any]:
    clean_snapshot_id = str(snapshot_id or "").strip()
    if not clean_snapshot_id:
        raise SnapshotError("Debes indicar el snapshot que quieres importar.")

    for snapshot in list_snapshots(verify_hash=True, include_invalid=True):
        if str(snapshot.get("snapshot_id") or "") != clean_snapshot_id:
            continue
        if not snapshot.get("valid"):
            error = str(snapshot.get("error") or "Snapshot no valido.")
            raise SnapshotError(f"No se puede importar `{clean_snapshot_id}`: {error}")
        return snapshot

    raise SnapshotError(f"No existe el snapshot `{clean_snapshot_id}`.")


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    for index in range(1, 1000):
        candidate = path.with_name(f"{stem}_{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise SnapshotError(f"No se pudo generar un nombre unico para `{path}`.")


def _backup_local_database(snapshot_id: str) -> Path | None:
    if not DB_PATH.exists():
        return None

    try:
        with duckdb.connect(str(DB_PATH)) as con:
            con.execute("CHECKPOINT")
    except Exception as exc:
        raise SnapshotError(f"No se pudo preparar la base local antes del backup: {exc}") from exc

    backup_dir = DB_PATH.parent / "backups" / "local"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _now().strftime("%Y%m%d_%H%M%S")
    backup_path = _unique_path(
        backup_dir / f"books_before_import_{timestamp}_{_slug(snapshot_id, 'snapshot')}.duckdb"
    )
    shutil.copy2(DB_PATH, backup_path)
    return backup_path


def import_snapshot(*, snapshot_id: str, confirm: bool = False) -> dict[str, Any]:
    if not confirm:
        raise SnapshotError("La importacion requiere confirm=true.")

    snapshot = _find_snapshot(snapshot_id)
    source_path = Path(str(snapshot["path"]))
    expected_sha = str(snapshot.get("sha256") or snapshot.get("actual_sha256") or "")
    actual_sha = _sha256_file(source_path)
    if expected_sha and actual_sha != expected_sha:
        raise SnapshotError("El hash sha256 del snapshot no coincide.")

    backup_path = _backup_local_database(str(snapshot["snapshot_id"]))
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_local_path = DB_PATH.with_suffix(DB_PATH.suffix + ".importing")
    if tmp_local_path.exists():
        tmp_local_path.unlink()

    try:
        shutil.copy2(source_path, tmp_local_path)
        if expected_sha and _sha256_file(tmp_local_path) != expected_sha:
            raise SnapshotError("La copia local del snapshot no conserva el sha256 esperado.")
        tmp_local_path.replace(DB_PATH)
    finally:
        if tmp_local_path.exists():
            tmp_local_path.unlink()

    state = _update_import_state(snapshot, backup_path)
    return {
        "ok": True,
        "snapshot": snapshot,
        "backup_path": str(backup_path) if backup_path else None,
        "sync_state": state,
    }


def cleanup_snapshots() -> dict[str, Any]:
    snapshots = list_snapshots(verify_hash=False, include_invalid=False)
    if not snapshots:
        return {"deleted": [], "kept": []}

    protected_ids: set[str] = set()
    for snapshot in snapshots[:SYNC_KEEP_MIN]:
        if snapshot.get("snapshot_id"):
            protected_ids.add(str(snapshot["snapshot_id"]))

    cutoff = _now() - timedelta(days=max(0, int(SYNC_RETENTION_DAYS)))
    deleted: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []

    for snapshot in snapshots:
        snapshot_id = str(snapshot.get("snapshot_id") or "")
        created_at = _snapshot_created_at(snapshot)
        should_keep = (
            bool(snapshot.get("protected"))
            or snapshot_id in protected_ids
            or created_at is None
            or created_at >= cutoff
        )
        if should_keep:
            kept.append(snapshot)
            continue

        db_path = Path(str(snapshot.get("path") or ""))
        manifest_path = Path(str(snapshot.get("manifest_path") or ""))
        removed_paths: list[str] = []
        for path in (db_path, manifest_path):
            if path.exists():
                path.unlink()
                removed_paths.append(str(path))
        deleted.append({"snapshot_id": snapshot_id, "paths": removed_paths})

    return {"deleted": deleted, "kept": kept}
