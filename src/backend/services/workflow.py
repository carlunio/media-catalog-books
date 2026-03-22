import importlib.util
from collections import defaultdict
from typing import Any

from ..config import WORKFLOW_MAX_ATTEMPTS
from . import books

VALID_STAGES = {"ocr", "metadata", "catalog", "cover"}
STAGE_BUCKETS = (
    "ocr",
    "metadata",
    "catalog",
    "cover",
    "review",
    "done",
    "running",
    "unknown",
)

WORKFLOW_GRAPH_NODES = [
    {"id": "load_book", "label": "Load book", "kind": "control"},
    {"id": "apply_action", "label": "Apply action", "kind": "control"},
    {"id": "ocr", "label": "OCR", "kind": "stage", "stage": "ocr"},
    {"id": "metadata", "label": "Metadata APIs", "kind": "stage", "stage": "metadata"},
    {"id": "catalog", "label": "Catalog build", "kind": "stage", "stage": "catalog"},
    {"id": "cover", "label": "Cover download", "kind": "stage", "stage": "cover"},
    {"id": "evaluate", "label": "Evaluate", "kind": "control"},
    {"id": "retry", "label": "Retry", "kind": "control"},
    {"id": "end", "label": "End", "kind": "terminal"},
]

WORKFLOW_GRAPH_EDGES = [
    {"source": "load_book", "target": "apply_action"},
    {"source": "apply_action", "target": "ocr"},
    {"source": "ocr", "target": "metadata"},
    {"source": "metadata", "target": "catalog"},
    {"source": "catalog", "target": "cover"},
    {"source": "cover", "target": "evaluate"},
    {"source": "evaluate", "target": "retry", "label": "route=retry"},
    {"source": "evaluate", "target": "end", "label": "route=end"},
    {"source": "retry", "target": "ocr"},
]

WORKFLOW_STAGE_TO_NODE = {
    "ocr": "ocr",
    "metadata": "metadata",
    "catalog": "catalog",
    "cover": "cover",
}


def is_langgraph_available() -> bool:
    return importlib.util.find_spec("langgraph") is not None


def _invoke_graph(initial_state: dict[str, Any]) -> dict[str, Any]:
    try:
        from ..workflow import run_workflow_graph
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "langgraph is not installed in the current environment. "
            "Install project dependencies and restart the backend."
        ) from exc

    return run_workflow_graph(initial_state)


def _normalize_stage(value: str | None, *, default: str) -> str:
    if not value:
        return default
    stage = value.strip().lower()
    if stage not in VALID_STAGES:
        raise ValueError(f"Invalid stage: {value}")
    return stage


def graph_definition() -> dict[str, Any]:
    return {
        "langgraph_available": is_langgraph_available(),
        "start_node": "load_book",
        "end_node": "end",
        "stage_order": ["ocr", "metadata", "catalog", "cover"],
        "stage_to_node": WORKFLOW_STAGE_TO_NODE,
        "nodes": WORKFLOW_GRAPH_NODES,
        "edges": WORKFLOW_GRAPH_EDGES,
    }


def _stage_bucket(stage: str | None) -> str:
    normalized = (stage or "").strip().lower()
    if not normalized:
        return "unknown"
    if normalized.startswith("running"):
        return "running"
    if normalized in STAGE_BUCKETS:
        return normalized
    return "unknown"


def snapshot(
    *,
    limit: int = 5000,
    review_limit: int = 200,
    block: str | None = None,
    module: str | None = None,
) -> dict[str, Any]:
    rows = books.list_books(limit=limit, block=block, module=module)
    stage_counts: dict[str, int] = defaultdict(int)
    workflow_status_counts: dict[str, int] = defaultdict(int)
    running_nodes: dict[str, int] = defaultdict(int)

    for row in rows:
        stage_counts[_stage_bucket(row.get("pipeline_stage"))] += 1
        status = str(row.get("workflow_status") or "pending").strip().lower() or "pending"
        workflow_status_counts[status] += 1
        if status == "running":
            node = str(row.get("workflow_current_node") or "unknown")
            running_nodes[node] += 1

    for bucket in STAGE_BUCKETS:
        stage_counts.setdefault(bucket, 0)

    review_rows = books.list_books(
        stage="needs_workflow_review",
        limit=review_limit,
        block=block,
        module=module,
    )
    review_queue = [
        {
            "id": row.get("id"),
            "pipeline_stage": row.get("pipeline_stage"),
            "workflow_current_node": row.get("workflow_current_node"),
            "workflow_review_reason": row.get("workflow_review_reason"),
            "workflow_attempt": row.get("workflow_attempt"),
            "updated_at": row.get("updated_at"),
        }
        for row in review_rows
    ]

    return {
        "scope": {"block": block, "module": module},
        "total_considered": len(rows),
        "stage_counts": dict(stage_counts),
        "workflow_status_counts": dict(workflow_status_counts),
        "running_nodes": dict(running_nodes),
        "review_queue_size": len(review_rows),
        "review_queue": review_queue,
    }


def run_one(
    book_id: str,
    *,
    start_stage: str = "ocr",
    stop_after: str | None = None,
    action: str | None = None,
    overwrite: bool = False,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
    ocr_provider: str | None = None,
    ocr_model: str | None = None,
    ocr_resize_to_1800: bool = False,
    catalog_provider: str | None = None,
    catalog_model: str | None = None,
) -> dict[str, Any]:
    stage = _normalize_stage(start_stage, default="ocr")
    stop = _normalize_stage(stop_after, default=stage) if stop_after else None

    if books.get_book(book_id) is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    result_state = _invoke_graph(
        {
            "book_id": book_id,
            "start_stage": stage,
            "stop_after": stop,
            "action": action,
            "overwrite": overwrite,
            "max_attempts": int(max_attempts),
            "ocr_provider": ocr_provider,
            "ocr_model": ocr_model,
            "ocr_resize_to_1800": bool(ocr_resize_to_1800),
            "catalog_provider": catalog_provider,
            "catalog_model": catalog_model,
            "stop_pipeline": False,
        }
    )

    book = books.get_book(book_id)

    if book is None:
        return {"id": book_id, "status": "error", "error": "Book disappeared after workflow run"}

    failed_step = result_state.get("failed_step")
    error = result_state.get("error")

    status = "ok"
    if book.get("workflow_status") == "review":
        status = "review"
    elif failed_step:
        status = "error"
    elif result_state.get("outcome") == "done":
        status = "done"
    elif result_state.get("outcome") == "approved":
        status = "approved"
    elif result_state.get("outcome") == "partial":
        status = "partial"

    return {
        "id": book_id,
        "status": status,
        "workflow_status": book.get("workflow_status"),
        "workflow_current_node": book.get("workflow_current_node"),
        "workflow_attempt": book.get("workflow_attempt"),
        "workflow_needs_review": book.get("workflow_needs_review"),
        "workflow_review_reason": book.get("workflow_review_reason"),
        "failed_step": failed_step,
        "error": error,
        "ocr_status": book.get("ocr_status"),
        "metadata_status": book.get("metadata_status"),
        "catalog_status": book.get("catalog_status"),
        "cover_status": book.get("cover_status"),
        "ocr_provider": book.get("ocr_provider"),
        "ocr_model": book.get("ocr_model"),
        "outcome": result_state.get("outcome"),
    }


def run_batch(
    *,
    book_id: str | None = None,
    block: str | None = None,
    module: str | None = None,
    limit: int = 20,
    start_stage: str = "ocr",
    stop_after: str | None = None,
    action: str | None = None,
    overwrite: bool = False,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
    ocr_provider: str | None = None,
    ocr_model: str | None = None,
    ocr_resize_to_1800: bool = False,
    catalog_provider: str | None = None,
    catalog_model: str | None = None,
) -> dict[str, Any]:
    stage = _normalize_stage(start_stage, default="ocr")
    stop = _normalize_stage(stop_after, default=stage) if stop_after else None

    scope_block, scope_module = books.resolve_scope(block, module, require=True)

    if book_id:
        target_book = books.get_book(book_id)
        if target_book is None:
            return {
                "scope": {"block": scope_block, "module": scope_module},
                "requested": 1,
                "processed": 1,
                "items": [{"id": book_id, "status": "error", "error": "Book not found"}],
            }

        target_block = str(target_book.get("block") or "").strip().upper()
        target_module = str(target_book.get("module") or "").strip().zfill(2)
        if target_block != scope_block or target_module != scope_module:
            raise ValueError(
                f"Book {book_id} is not in selected scope {scope_block}/{scope_module} "
                f"(book scope is {target_block}/{target_module})"
            )

        if not overwrite:
            current_stage = str(target_book.get("pipeline_stage") or "").strip().lower()
            if current_stage != stage:
                return {
                    "scope": {"block": scope_block, "module": scope_module},
                    "requested": 1,
                    "processed": 1,
                    "items": [
                        {
                            "id": book_id,
                            "status": "skipped",
                            "reason": (
                                f"Book stage '{current_stage or 'unknown'}' does not match "
                                f"start_stage '{stage}' with overwrite disabled"
                            ),
                        }
                    ],
                }

        targets = [book_id]
    else:
        targets = books.book_ids_for_workflow(
            limit=limit,
            start_stage=stage,
            overwrite=overwrite,
            block=scope_block,
            module=scope_module,
        )

    items: list[dict[str, Any]] = []
    for target_id in targets:
        items.append(
            run_one(
                target_id,
                start_stage=stage,
                stop_after=stop,
                action=action,
                overwrite=overwrite,
                max_attempts=max_attempts,
                ocr_provider=ocr_provider,
                ocr_model=ocr_model,
                ocr_resize_to_1800=ocr_resize_to_1800,
                catalog_provider=catalog_provider,
                catalog_model=catalog_model,
            )
        )

    return {
        "scope": {"block": scope_block, "module": scope_module},
        "requested": len(targets),
        "processed": len(items),
        "items": items,
    }


def eligible_count(
    *,
    start_stage: str = "ocr",
    overwrite: bool = False,
    block: str | None = None,
    module: str | None = None,
) -> dict[str, Any]:
    stage = _normalize_stage(start_stage, default="ocr")
    scope_block, scope_module = books.resolve_scope(block, module, require=True)
    eligible = books.count_books_for_stage(
        stage=stage,
        overwrite=overwrite,
        block=scope_block,
        module=scope_module,
    )
    return {
        "scope": {"block": scope_block, "module": scope_module},
        "start_stage": stage,
        "overwrite": bool(overwrite),
        "eligible": int(eligible),
    }


def _review_origin_stage(book: dict[str, Any]) -> str | None:
    node = str(book.get("workflow_current_node") or "").strip().lower()
    reason = str(book.get("workflow_review_reason") or "").strip().lower()

    if node.startswith("stage:"):
        node = node.split(":", 1)[1].strip()
    if node.startswith("retry_"):
        node = node.split("_", 1)[1].strip()

    for stage in ("ocr", "metadata", "catalog", "cover"):
        if node == stage or node.startswith(f"{stage}_") or node.startswith(f"{stage}:"):
            return stage

    for stage in ("ocr", "metadata", "catalog", "cover"):
        if reason.startswith(stage):
            return stage

    return None


def _mark_stage_as_manually_approved(book: dict[str, Any], *, stage: str) -> None:
    book_id = str(book.get("id") or "").strip()
    if not book_id:
        raise ValueError("Book id is missing")

    if stage == "ocr":
        trace_payload = book.get("ocr_trace")
        if not isinstance(trace_payload, (dict, list)):
            trace_payload = {}
        books.update_ocr(
            book_id,
            credits_text=str(book.get("credits_text") or "").strip() or None,
            isbn_raw=str(book.get("isbn_raw") or "").strip() or None,
            isbn=str(book.get("isbn") or "").strip() or None,
            status="manual",
            provider=str(book.get("ocr_provider") or "").strip() or "manual",
            model=str(book.get("ocr_model") or "").strip() or None,
            trace=trace_payload,
            error=None,
        )
        return

    if stage == "metadata":
        metadata_payload = book.get("metadata")
        books.update_metadata(
            book_id,
            metadata=metadata_payload if isinstance(metadata_payload, dict) else {},
            status="manual",
            error=None,
        )
        return

    if stage == "catalog":
        catalog_payload = book.get("catalog")
        books.update_catalog(
            book_id,
            catalog=catalog_payload if isinstance(catalog_payload, dict) else {},
            status="manual",
            error=None,
        )
        return

    if stage == "cover":
        books.update_cover(
            book_id,
            cover_path=str(book.get("cover_path") or "").strip() or None,
            status="skipped",
            error=None,
        )
        return

    raise ValueError(f"Unsupported stage for manual approval: {stage}")


def _approve_review_without_running(book_id: str) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        raise ValueError("Book not found")

    if not bool(book.get("workflow_needs_review")):
        return {
            "id": book_id,
            "status": "skipped",
            "reason": "Book is not in review state",
            "pipeline_stage": book.get("pipeline_stage"),
        }

    origin_stage = _review_origin_stage(book)
    if origin_stage:
        _mark_stage_as_manually_approved(book, stage=origin_stage)

    books.clear_workflow_review(book_id)
    updated = books.get_book(book_id)
    if updated is None:
        return {
            "id": book_id,
            "status": "error",
            "error": "Book disappeared after review approval",
        }

    target_stage = str(updated.get("pipeline_stage") or "").strip().lower()
    if target_stage == "done":
        books.set_workflow_done(book_id, node="review_approved")
    elif target_stage and not target_stage.startswith("running"):
        books.set_workflow_pending(book_id, node=f"stage:{target_stage}", reason=None)

    final_book = books.get_book(book_id) or updated
    return {
        "id": book_id,
        "status": "approved",
        "origin_stage": origin_stage,
        "target_stage": str(final_book.get("pipeline_stage") or "").strip().lower() or None,
        "workflow_status": final_book.get("workflow_status"),
        "workflow_current_node": final_book.get("workflow_current_node"),
        "workflow_needs_review": final_book.get("workflow_needs_review"),
        "workflow_review_reason": final_book.get("workflow_review_reason"),
    }


def review_action(
    book_id: str,
    *,
    action: str,
    max_attempts: int = WORKFLOW_MAX_ATTEMPTS,
    ocr_provider: str | None = None,
    ocr_model: str | None = None,
    ocr_resize_to_1800: bool = False,
    catalog_provider: str | None = None,
    catalog_model: str | None = None,
) -> dict[str, Any]:
    normalized = action.strip().lower()
    if normalized == "approve":
        return _approve_review_without_running(book_id)

    action_to_stage = {
        "retry_from_ocr": "ocr",
        "retry_from_metadata": "metadata",
        "retry_from_catalog": "catalog",
        "retry_from_cover": "cover",
    }

    stage = action_to_stage.get(normalized)
    if stage is None:
        raise ValueError(f"Invalid review action: {action}")

    return run_one(
        book_id,
        start_stage=stage,
        stop_after=None,
        action=normalized,
        overwrite=True,
        max_attempts=max_attempts,
        ocr_provider=ocr_provider,
        ocr_model=ocr_model,
        ocr_resize_to_1800=ocr_resize_to_1800,
        catalog_provider=catalog_provider,
        catalog_model=catalog_model,
    )


def mark_review(book_id: str, *, reason: str | None = None, node: str = "manual") -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        raise ValueError("Book not found")

    text = (reason or "").strip()
    if not text:
        text = "Marked for manual review from Streamlit orchestration page"

    books.set_workflow_review(book_id, node=node.strip() or "manual", reason=text, error=None)
    updated = books.get_book(book_id)
    return {
        "id": book_id,
        "status": "review",
        "workflow_status": updated.get("workflow_status") if updated else None,
        "workflow_needs_review": updated.get("workflow_needs_review") if updated else True,
        "workflow_review_reason": updated.get("workflow_review_reason") if updated else text,
        "pipeline_stage": updated.get("pipeline_stage") if updated else "review",
    }
