from src.backend.services import workflow


def test_batch_continues_after_one_item_raises(monkeypatch):
    calls = []
    review_calls = []

    monkeypatch.setattr(
        workflow.books,
        "resolve_scope",
        lambda block, module, require: ("A", "01"),
    )
    monkeypatch.setattr(
        workflow.books,
        "book_ids_for_workflow",
        lambda **kwargs: ["01A0001", "01A0002"],
    )
    monkeypatch.setattr(
        workflow.books,
        "set_workflow_review",
        lambda book_id, **kwargs: review_calls.append((book_id, kwargs)),
    )

    def fake_run_one(book_id, **kwargs):
        calls.append((book_id, kwargs))
        if book_id == "01A0001":
            raise TimeoutError("item timed out")
        return {"id": book_id, "status": "partial"}

    monkeypatch.setattr(workflow, "run_one", fake_run_one)

    result = workflow.run_batch(
        block="A",
        module="01",
        limit=2,
        start_stage="ocr",
        stop_after="ocr",
    )

    assert [book_id for book_id, _kwargs in calls] == ["01A0001", "01A0002"]
    assert all(kwargs["download_cover_after_metadata"] is True for _, kwargs in calls)
    assert result["processed"] == 2
    assert result["items"][0] == {
        "id": "01A0001",
        "status": "error",
        "failed_step": "ocr",
        "error": "item timed out",
    }
    assert result["items"][1] == {"id": "01A0002", "status": "partial"}
    assert review_calls == [
        (
            "01A0001",
            {
                "node": "ocr",
                "reason": "ocr: item timed out",
                "error": "item timed out",
            },
        )
    ]


def test_metadata_batch_runs_full_cover_sweep_with_zero_metadata_targets(monkeypatch):
    sweep_calls = []

    monkeypatch.setattr(
        workflow.books,
        "resolve_scope",
        lambda block, module, require: ("A", "01"),
    )
    monkeypatch.setattr(
        workflow.books,
        "book_ids_for_workflow",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        workflow,
        "run_cover_batch",
        lambda **kwargs: sweep_calls.append(kwargs)
        or {
            "branch": "cover",
            "requested": 3,
            "processed": 3,
            "items": [],
        },
    )

    result = workflow.run_batch(
        block="A",
        module="01",
        limit=1,
        start_stage="metadata",
        stop_after="metadata",
        download_cover_after_metadata=True,
    )

    assert result["requested"] == 0
    assert result["processed"] == 0
    assert result["cover_sweep"]["processed"] == 3
    assert sweep_calls == [
        {
            "block": "A",
            "module": "01",
            "limit": None,
            "overwrite": False,
        }
    ]


def test_metadata_batch_can_disable_full_cover_sweep(monkeypatch):
    monkeypatch.setattr(
        workflow.books,
        "resolve_scope",
        lambda block, module, require: ("A", "01"),
    )
    monkeypatch.setattr(
        workflow.books,
        "book_ids_for_workflow",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        workflow,
        "run_cover_batch",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("cover sweep should be disabled")
        ),
    )

    result = workflow.run_batch(
        block="A",
        module="01",
        limit=1,
        start_stage="metadata",
        stop_after="metadata",
        download_cover_after_metadata=False,
    )

    assert result["cover_sweep"] is None


def test_cover_batch_selects_every_metadata_book_without_a_physical_file(
    monkeypatch,
):
    cover_calls = []
    query_calls = []

    monkeypatch.setattr(
        workflow.books,
        "resolve_scope",
        lambda block, module, require: ("A", "01"),
    )
    monkeypatch.setattr(
        workflow.books,
        "count_books_for_cover_download",
        lambda **kwargs: 3,
    )

    def fake_ids(**kwargs):
        query_calls.append(kwargs)
        return ["01A0001", "01A0002", "01A0003"]

    monkeypatch.setattr(
        workflow.books,
        "book_ids_for_cover_download",
        fake_ids,
    )
    monkeypatch.setattr(
        workflow.covers,
        "existing_cover_file",
        lambda book_id: "existing.jpg" if book_id == "01A0002" else None,
    )
    monkeypatch.setattr(
        workflow,
        "run_cover_one",
        lambda book_id, **kwargs: cover_calls.append((book_id, kwargs))
        or {"id": book_id, "status": "downloaded"},
    )

    result = workflow.run_cover_batch(
        block="A",
        module="01",
        limit=None,
        overwrite=False,
    )

    assert query_calls == [
        {
            "limit": 3,
            "overwrite": True,
            "block": "A",
            "module": "01",
        }
    ]
    assert cover_calls == [
        ("01A0001", {"overwrite": False}),
        ("01A0003", {"overwrite": False}),
    ]
    assert result["requested"] == 2
    assert result["processed"] == 2
