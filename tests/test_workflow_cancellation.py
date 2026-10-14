from src.backend.workflow import graph


def test_stop_requested_during_ocr_restores_a_pending_valid_stage(monkeypatch):
    running_calls = []
    pending_calls = []
    stop_checks = iter((False, True))
    book = {
        "id": "01A0001",
        "ocr_status": "processed",
        "credits_text": "Texto reconocido",
        "isbn": "9788400000000",
    }

    monkeypatch.setattr(
        graph.books,
        "set_workflow_running",
        lambda book_id, **kwargs: running_calls.append((book_id, kwargs)),
    )
    monkeypatch.setattr(graph.ocr, "run_one", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(graph.books, "get_book", lambda _book_id: book)
    monkeypatch.setattr(
        graph.books,
        "set_workflow_pending",
        lambda book_id, **kwargs: pending_calls.append((book_id, kwargs)),
    )

    result = graph._ocr_node(
        {
            "book_id": "01A0001",
            "start_stage": "ocr",
            "stop_requested": lambda: next(stop_checks),
        }
    )

    assert running_calls
    assert result["stop_pipeline"] is True
    assert result["outcome"] == "stopping"

    evaluated = graph._evaluate_node({"book_id": "01A0001", **result})

    assert evaluated == {"route": "end", "outcome": "partial"}
    assert pending_calls == [
        ("01A0001", {"node": "stopped", "reason": "Stopped by user"})
    ]


def test_ocr_timeout_goes_directly_to_review_with_specific_cause(monkeypatch):
    review_calls = []
    book = {"id": "01A0001", "ocr_status": "error"}

    monkeypatch.setattr(
        graph.books, "set_workflow_running", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        graph.ocr,
        "run_one",
        lambda *args, **kwargs: {
            "status": "error",
            "error_code": "ocr_timeout",
            "error": "El OCR superó 60 segundos.",
            "review_reason": (
                "ocr_timeout: una imagen superó 60 segundos. "
                "Recorta manualmente la fotografía y vuelve a ejecutar OCR."
            ),
        },
    )
    monkeypatch.setattr(graph.books, "get_book", lambda _book_id: book)
    monkeypatch.setattr(
        graph.books,
        "set_workflow_review",
        lambda book_id, **kwargs: review_calls.append((book_id, kwargs)),
    )

    result = graph._ocr_node({"book_id": "01A0001", "start_stage": "ocr"})

    assert result["outcome"] == "review"
    assert result["stop_pipeline"] is True
    assert review_calls == [
        (
            "01A0001",
            {
                "node": "ocr_timeout",
                "reason": (
                    "ocr_timeout: una imagen superó 60 segundos. "
                    "Recorta manualmente la fotografía y vuelve a ejecutar OCR."
                ),
                "error": "El OCR superó 60 segundos.",
            },
        )
    ]
