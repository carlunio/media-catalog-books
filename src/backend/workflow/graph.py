from typing import Any, Literal, TypedDict

from langgraph.graph import END, StateGraph

from ..config import WORKFLOW_MAX_ATTEMPTS
from ..normalizers import is_valid_isbn
from ..services import books, catalog, covers, metadata, ocr

StageName = Literal["ocr", "metadata", "catalog", "cover"]

STAGE_ORDER: dict[StageName, int] = {
    "ocr": 1,
    "metadata": 2,
    "catalog": 3,
    "cover": 4,
}

RETRY_STAGE_MAP: dict[str, StageName] = {
    "ocr": "ocr",
    "metadata": "metadata",
    "catalog": "catalog",
    "cover": "cover",
}


class WorkflowState(TypedDict, total=False):
    book_id: str
    book: dict[str, Any] | None

    start_stage: StageName
    stop_after: StageName | None
    action: str | None
    overwrite: bool
    ocr_provider: str | None
    ocr_model: str | None

    max_attempts: int

    attempt: int
    failed_step: str | None
    error: str | None

    stop_pipeline: bool
    outcome: str
    route: Literal["retry", "end"]


_GRAPH = None


def _stage_enabled(state: WorkflowState, stage: StageName) -> bool:
    start_stage = state.get("start_stage", "ocr")
    start_idx = STAGE_ORDER.get(start_stage, 1)
    return STAGE_ORDER[stage] >= start_idx


def _should_stop_after(state: WorkflowState, stage: StageName) -> bool:
    return state.get("stop_after") == stage


def _with_failure(book_id: str, *, step: str, error: str) -> WorkflowState:
    books.set_workflow_error(book_id, node=step, error=error)
    return {
        "failed_step": step,
        "error": error,
    }


def _invalid_ocr_isbn_reason(book: dict[str, Any]) -> str:
    reason = "ocr_isbn_validation: OCR text extracted but no valid ISBN was found"
    trace = book.get("ocr_trace") if isinstance(book.get("ocr_trace"), dict) else {}
    extraction = trace.get("isbn_extraction") if isinstance(trace.get("isbn_extraction"), dict) else {}

    candidates: list[str] = []
    raw_candidates = extraction.get("candidates")
    if isinstance(raw_candidates, list):
        candidates = [str(item) for item in raw_candidates if str(item).strip()]
    else:
        legacy_result = extraction.get("result") if isinstance(extraction.get("result"), dict) else {}
        legacy_candidates = legacy_result.get("isbns")
        if isinstance(legacy_candidates, list):
            candidates = [str(item) for item in legacy_candidates if str(item).strip()]

    isbn_raw = str(book.get("isbn_raw") or "").strip()

    tail_parts: list[str] = []
    if isbn_raw:
        tail_parts.append(f"isbn_raw={isbn_raw}")
    if candidates:
        sample = ",".join(str(item) for item in candidates[:4])
        tail_parts.append(f"candidates={sample}")

    if tail_parts:
        reason = f"{reason} ({'; '.join(tail_parts)})"

    return reason[:400]


def _should_route_to_ocr_review(book: dict[str, Any] | None) -> bool:
    if not isinstance(book, dict):
        return False

    ocr_status = str(book.get("ocr_status") or "").strip().lower()
    credits_text = str(book.get("credits_text") or "").strip()
    isbn_text = str(book.get("isbn") or "").strip()
    return ocr_status in {"processed", "manual"} and bool(credits_text) and not is_valid_isbn(isbn_text)


def _load_book_node(state: WorkflowState) -> WorkflowState:
    book_id = state["book_id"]
    book = books.get_book(book_id)

    if book is None:
        return {
            "failed_step": "load_book",
            "error": f"Book not found: {book_id}",
            "stop_pipeline": True,
            "route": "end",
        }

    books.set_workflow_running(book_id, node="load_book", action=state.get("action"))
    return {
        "book": book,
        "attempt": int(book.get("workflow_attempt") or 0),
    }


def _apply_action_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step"):
        return {}

    book_id = state["book_id"]
    action = (state.get("action") or "").strip().lower()

    if not action or action == "none":
        return {}

    books.set_workflow_running(book_id, node="apply_action", action=action)

    if action == "approve":
        books.clear_workflow_review(book_id)
        books.set_workflow_done(book_id, node="review_approved")
        return {
            "stop_pipeline": True,
            "outcome": "approved",
        }

    retry_action_to_stage: dict[str, StageName] = {
        "retry_from_ocr": "ocr",
        "retry_from_metadata": "metadata",
        "retry_from_catalog": "catalog",
        "retry_from_cover": "cover",
    }

    retry_stage = retry_action_to_stage.get(action)
    if retry_stage is None:
        return _with_failure(book_id, step="apply_action", error=f"Unsupported action: {action}")

    attempt = books.increment_workflow_attempt(book_id)
    books.reset_from_stage(book_id, retry_stage)
    books.clear_workflow_review(book_id)

    refreshed = books.get_book(book_id)
    return {
        "attempt": attempt,
        "book": refreshed,
        "start_stage": retry_stage,
        "overwrite": True,
        "failed_step": None,
        "error": None,
        "stop_pipeline": False,
        "outcome": "retry",
    }


def _ocr_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "ocr"):
        return {}

    book_id = state["book_id"]
    books.set_workflow_running(book_id, node="ocr", action=state.get("action"))

    result = ocr.run_one(
        book_id,
        provider=state.get("ocr_provider"),
        model=state.get("ocr_model"),
        overwrite=bool(state.get("overwrite")),
    )
    if str(result.get("status") or "").strip().lower() == "error":
        return _with_failure(book_id, step="ocr", error=str(result.get("error") or "OCR failed"))

    refreshed = books.get_book(book_id)
    if _should_route_to_ocr_review(refreshed):
        books.set_workflow_review(
            book_id,
            node="ocr_isbn_validation",
            reason=_invalid_ocr_isbn_reason(refreshed or {}),
            error=None,
        )
        reviewed = books.get_book(book_id)
        return {
            "book": reviewed,
            "stop_pipeline": True,
            "outcome": "review",
        }

    if _should_stop_after(state, "ocr"):
        return {
            "book": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_ocr",
        }

    return {"book": refreshed}


def _metadata_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "metadata"):
        return {}

    book_id = state["book_id"]
    books.set_workflow_running(book_id, node="metadata", action=state.get("action"))

    result = metadata.run_one(book_id, overwrite=bool(state.get("overwrite")))
    if str(result.get("status") or "").strip().lower() == "error":
        return _with_failure(book_id, step="metadata", error=str(result.get("error") or "Metadata fetch failed"))

    refreshed = books.get_book(book_id)
    if _should_stop_after(state, "metadata"):
        return {
            "book": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_metadata",
        }

    return {"book": refreshed}


def _catalog_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "catalog"):
        return {}

    book_id = state["book_id"]
    books.set_workflow_running(book_id, node="catalog", action=state.get("action"))

    result = catalog.run_one(book_id, overwrite=bool(state.get("overwrite")))
    if str(result.get("status") or "").strip().lower() == "error":
        return _with_failure(book_id, step="catalog", error=str(result.get("error") or "Catalog build failed"))

    refreshed = books.get_book(book_id)
    if _should_stop_after(state, "catalog"):
        return {
            "book": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_catalog",
        }

    return {"book": refreshed}


def _cover_node(state: WorkflowState) -> WorkflowState:
    if state.get("failed_step") or state.get("stop_pipeline"):
        return {}

    if not _stage_enabled(state, "cover"):
        return {}

    book_id = state["book_id"]
    books.set_workflow_running(book_id, node="cover", action=state.get("action"))

    result = covers.run_one(book_id, overwrite=bool(state.get("overwrite")))
    if str(result.get("status") or "").strip().lower() == "error":
        return _with_failure(book_id, step="cover", error=str(result.get("error") or "Cover download failed"))

    refreshed = books.get_book(book_id)
    if _should_stop_after(state, "cover"):
        return {
            "book": refreshed,
            "stop_pipeline": True,
            "outcome": "stopped_after_cover",
        }

    return {"book": refreshed}


def _evaluate_node(state: WorkflowState) -> WorkflowState:
    book_id = state["book_id"]

    if state.get("outcome") == "approved":
        return {"route": "end"}
    if state.get("outcome") == "review":
        return {"route": "end"}

    failed_step = state.get("failed_step")
    error = state.get("error")

    if failed_step:
        if failed_step == "load_book":
            return {"route": "end"}

        attempt = int(state.get("attempt") or 0)
        max_attempts_raw = state.get("max_attempts")
        max_attempts = WORKFLOW_MAX_ATTEMPTS if max_attempts_raw is None else int(max_attempts_raw)

        if failed_step != "apply_action" and attempt < max_attempts:
            return {"route": "retry"}

        review_reason = f"{failed_step}: {error or 'Unknown error'}"
        books.set_workflow_review(
            book_id,
            node=failed_step,
            reason=review_reason,
            error=error,
        )
        return {
            "route": "end",
            "outcome": "review",
        }

    if state.get("stop_pipeline"):
        stop_after = state.get("stop_after")
        if stop_after:
            books.set_workflow_pending(
                book_id,
                node=f"stage:{stop_after}",
                reason=f"Stopped after stage {stop_after}",
            )
        else:
            books.set_workflow_pending(book_id, node="paused")

        return {
            "route": "end",
            "outcome": "partial",
        }

    # Automatic review routing for low-confidence catalog outputs.
    book = books.get_book(book_id)
    if book is not None:
        # OCR gate: if we have OCR text but ISBN is missing/invalid, force manual review.
        if _should_route_to_ocr_review(book):
            books.set_workflow_review(
                book_id,
                node="ocr_isbn_validation",
                reason=_invalid_ocr_isbn_reason(book),
                error=None,
            )
            return {
                "route": "end",
                "outcome": "review",
            }

        catalog_payload = book.get("catalog") if isinstance(book.get("catalog"), dict) else {}
        qa_payload = catalog_payload.get("qa") if isinstance(catalog_payload.get("qa"), dict) else {}
        if bool(qa_payload.get("requires_manual_review")):
            confidence = qa_payload.get("confidence")
            review_flags = qa_payload.get("review_flags") if isinstance(qa_payload.get("review_flags"), list) else []
            flags_text = ", ".join(str(flag) for flag in review_flags[:6]) if review_flags else "low catalog confidence"
            reason = f"catalog_quality confidence={confidence} flags={flags_text}"
            books.set_workflow_review(
                book_id,
                node="catalog_quality",
                reason=reason,
                error=None,
            )
            return {
                "route": "end",
                "outcome": "review",
            }

    books.set_workflow_done(book_id, node="workflow_done")
    return {
        "route": "end",
        "outcome": "done",
    }


def _retry_node(state: WorkflowState) -> WorkflowState:
    book_id = state["book_id"]
    failed_step = state.get("failed_step") or "ocr"
    retry_stage = RETRY_STAGE_MAP.get(failed_step, "ocr")

    attempt = books.increment_workflow_attempt(book_id)
    books.reset_from_stage(book_id, retry_stage)
    books.set_workflow_running(book_id, node=f"retry_{retry_stage}", action="auto_retry")

    refreshed = books.get_book(book_id)

    return {
        "book": refreshed,
        "attempt": attempt,
        "start_stage": retry_stage,
        "failed_step": None,
        "error": None,
        "stop_pipeline": False,
        "outcome": "retry",
        "overwrite": True,
    }


def _route_after_evaluate(state: WorkflowState) -> Literal["retry", "end"]:
    return state.get("route", "end")


def _build_graph():
    builder = StateGraph(WorkflowState)

    builder.add_node("load_book", _load_book_node)
    builder.add_node("apply_action", _apply_action_node)
    builder.add_node("ocr", _ocr_node)
    builder.add_node("metadata", _metadata_node)
    builder.add_node("catalog", _catalog_node)
    builder.add_node("cover", _cover_node)
    builder.add_node("evaluate", _evaluate_node)
    builder.add_node("retry", _retry_node)

    builder.set_entry_point("load_book")
    builder.add_edge("load_book", "apply_action")
    builder.add_edge("apply_action", "ocr")
    builder.add_edge("ocr", "metadata")
    builder.add_edge("metadata", "catalog")
    builder.add_edge("catalog", "cover")
    builder.add_edge("cover", "evaluate")

    builder.add_conditional_edges(
        "evaluate",
        _route_after_evaluate,
        {
            "retry": "retry",
            "end": END,
        },
    )

    builder.add_edge("retry", "ocr")

    return builder.compile()


def get_workflow_graph():
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_graph()
    return _GRAPH


def run_workflow_graph(initial_state: WorkflowState) -> WorkflowState:
    graph = get_workflow_graph()
    return graph.invoke(initial_state)
