from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Event, Lock, Thread
from typing import Any
from uuid import uuid4

from . import books, workflow

_ACTIVE_STATUSES = {"queued", "running", "stopping"}
_TERMINAL_STATUSES = {"completed", "stopped", "failed"}
_MAX_HISTORY = 100

_LOCK = Lock()
_JOBS: dict[str, dict[str, Any]] = {}
_ACTIVE_BY_SCOPE: dict[tuple[str, str], str] = {}


class WorkflowJobConflict(RuntimeError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _public_job(record: dict[str, Any], *, include_result: bool) -> dict[str, Any]:
    output = {
        key: deepcopy(value)
        for key, value in record.items()
        if not key.startswith("_") and key != "result"
    }
    if include_result and record.get("result") is not None:
        output["result"] = deepcopy(record["result"])
    return output


def _trim_history_locked() -> None:
    if len(_JOBS) <= _MAX_HISTORY:
        return
    terminal_ids = [
        job_id
        for job_id, record in _JOBS.items()
        if record.get("status") in _TERMINAL_STATUSES
    ]
    for job_id in terminal_ids[: max(0, len(_JOBS) - _MAX_HISTORY)]:
        _JOBS.pop(job_id, None)


def _update_progress(
    job_id: str,
    phase: str,
    processed: int,
    requested: int,
    last_item: dict[str, Any] | None,
) -> None:
    with _LOCK:
        record = _JOBS.get(job_id)
        if record is None:
            return
        record["phase"] = phase
        record["processed"] = int(processed)
        record["requested"] = int(requested)
        record["last_item"] = last_item
        record.setdefault("progress", {})[phase] = {
            "processed": int(processed),
            "requested": int(requested),
            "last_item": last_item,
        }
        record["updated_at"] = _now()


def _run_job(job_id: str, kwargs: dict[str, Any]) -> None:
    with _LOCK:
        record = _JOBS[job_id]
        if record["status"] == "queued":
            record["status"] = "running"
        record["started_at"] = _now()
        record["updated_at"] = record["started_at"]
        stop_event = record["_stop_event"]

    try:
        result = workflow.run_batch(
            **kwargs,
            stop_requested=stop_event.is_set,
            progress_callback=lambda phase, processed, requested, last_item: (
                _update_progress(
                    job_id,
                    phase,
                    processed,
                    requested,
                    last_item,
                )
            ),
        )
    except Exception as exc:
        with _LOCK:
            record = _JOBS[job_id]
            record["status"] = "failed"
            record["error"] = str(exc) or exc.__class__.__name__
            record["finished_at"] = _now()
            record["updated_at"] = record["finished_at"]
    else:
        with _LOCK:
            record = _JOBS[job_id]
            record["result"] = result
            record["processed"] = int(result.get("processed") or 0)
            record["requested"] = int(result.get("requested") or 0)
            record["status"] = "stopped" if result.get("stopped") else "completed"
            record["finished_at"] = _now()
            record["updated_at"] = record["finished_at"]
    finally:
        with _LOCK:
            record = _JOBS[job_id]
            scope_key = (record["block"], record["module"])
            if _ACTIVE_BY_SCOPE.get(scope_key) == job_id:
                _ACTIVE_BY_SCOPE.pop(scope_key, None)
            _trim_history_locked()


def start_job(**kwargs: Any) -> dict[str, Any]:
    scope_block, scope_module = books.resolve_scope(
        kwargs.get("block"),
        kwargs.get("module"),
        require=True,
    )
    assert scope_block is not None
    assert scope_module is not None
    scope_key = (scope_block, scope_module)

    normalized_kwargs = dict(kwargs)
    normalized_kwargs["block"] = scope_block
    normalized_kwargs["module"] = scope_module

    with _LOCK:
        active_id = _ACTIVE_BY_SCOPE.get(scope_key)
        if active_id:
            active = _JOBS.get(active_id)
            if active and active.get("status") in _ACTIVE_STATUSES:
                raise WorkflowJobConflict(
                    f"Ya hay una ejecución activa para {scope_block}/{scope_module}."
                )
            _ACTIVE_BY_SCOPE.pop(scope_key, None)

        job_id = uuid4().hex
        created_at = _now()
        start_stage = str(normalized_kwargs.get("start_stage") or "ocr")
        record: dict[str, Any] = {
            "job_id": job_id,
            "block": scope_block,
            "module": scope_module,
            "status": "queued",
            "phase": "cover" if start_stage == "cover" else "workflow",
            "start_stage": start_stage,
            "stop_after": normalized_kwargs.get("stop_after"),
            "download_cover_after_metadata": bool(
                normalized_kwargs.get("download_cover_after_metadata", True)
            ),
            "requested": 0,
            "processed": 0,
            "last_item": None,
            "progress": {},
            "error": None,
            "created_at": created_at,
            "started_at": None,
            "finished_at": None,
            "stop_requested_at": None,
            "updated_at": created_at,
            "result": None,
            "_stop_event": Event(),
        }
        _JOBS[job_id] = record
        _ACTIVE_BY_SCOPE[scope_key] = job_id
        thread = Thread(
            target=_run_job,
            args=(job_id, normalized_kwargs),
            name=f"workflow-{scope_block}-{scope_module}-{job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return _public_job(record, include_result=False)


def current_job(*, block: str, module: str) -> dict[str, Any] | None:
    scope_block, scope_module = books.resolve_scope(block, module, require=True)
    assert scope_block is not None
    assert scope_module is not None
    with _LOCK:
        job_id = _ACTIVE_BY_SCOPE.get((scope_block, scope_module))
        if not job_id:
            return None
        record = _JOBS.get(job_id)
        if record is None or record.get("status") not in _ACTIVE_STATUSES:
            return None
        return _public_job(record, include_result=False)


def get_job(job_id: str) -> dict[str, Any]:
    with _LOCK:
        record = _JOBS.get(str(job_id))
        if record is None:
            raise KeyError(job_id)
        return _public_job(record, include_result=True)


def request_stop(job_id: str) -> dict[str, Any]:
    with _LOCK:
        record = _JOBS.get(str(job_id))
        if record is None:
            raise KeyError(job_id)
        if record.get("status") in _ACTIVE_STATUSES:
            record["_stop_event"].set()
            record["status"] = "stopping"
            record["stop_requested_at"] = _now()
            record["updated_at"] = record["stop_requested_at"]
        return _public_job(record, include_result=False)
