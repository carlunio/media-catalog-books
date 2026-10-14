import time
from threading import Event

import pytest

from src.backend.services import workflow_jobs


@pytest.fixture(autouse=True)
def clean_job_registry():
    with workflow_jobs._LOCK:
        workflow_jobs._JOBS.clear()
        workflow_jobs._ACTIVE_BY_SCOPE.clear()
    yield
    with workflow_jobs._LOCK:
        active_records = [
            record
            for record in workflow_jobs._JOBS.values()
            if record.get("status") in workflow_jobs._ACTIVE_STATUSES
        ]
        for record in active_records:
            record["_stop_event"].set()


def test_background_job_can_be_stopped_and_blocks_parallel_scope(monkeypatch):
    started = Event()

    monkeypatch.setattr(
        workflow_jobs.books,
        "resolve_scope",
        lambda block, module, require: (str(block), str(module)),
    )

    def fake_run_batch(**kwargs):
        progress_callback = kwargs["progress_callback"]
        stop_requested = kwargs["stop_requested"]
        progress_callback("workflow", 0, 3, None)
        started.set()
        deadline = time.monotonic() + 2
        while not stop_requested() and time.monotonic() < deadline:
            time.sleep(0.005)
        progress_callback(
            "workflow",
            1,
            3,
            {"id": "01A0001", "status": "partial"},
        )
        return {
            "scope": {"block": "A", "module": "01"},
            "requested": 3,
            "processed": 1,
            "items": [{"id": "01A0001", "status": "partial"}],
            "cover_sweep": None,
            "stopped": True,
        }

    monkeypatch.setattr(workflow_jobs.workflow, "run_batch", fake_run_batch)

    job = workflow_jobs.start_job(
        block="A",
        module="01",
        limit=3,
        start_stage="metadata",
        stop_after="catalog",
        download_cover_after_metadata=True,
    )
    job_id = str(job["job_id"])
    assert job["start_stage"] == "metadata"
    assert job["stop_after"] == "catalog"
    assert job["download_cover_after_metadata"] is True
    assert started.wait(timeout=1)

    current = workflow_jobs.current_job(block="A", module="01")
    assert current is not None
    assert current["job_id"] == job_id
    assert current["status"] in {"running", "stopping"}
    assert current["progress"]["workflow"]["requested"] == 3

    with pytest.raises(workflow_jobs.WorkflowJobConflict):
        workflow_jobs.start_job(block="A", module="01", limit=1)

    stopping = workflow_jobs.request_stop(job_id)
    assert stopping["status"] == "stopping"
    assert stopping["stop_requested_at"]

    deadline = time.monotonic() + 2
    final = workflow_jobs.get_job(job_id)
    while final["status"] not in {"completed", "stopped", "failed"}:
        assert time.monotonic() < deadline
        time.sleep(0.01)
        final = workflow_jobs.get_job(job_id)

    assert final["status"] == "stopped"
    assert final["processed"] == 1
    assert final["requested"] == 3
    assert final["result"]["stopped"] is True
    assert workflow_jobs.current_job(block="A", module="01") is None


def test_background_job_keeps_workflow_and_cover_progress_separate(monkeypatch):
    monkeypatch.setattr(
        workflow_jobs.books,
        "resolve_scope",
        lambda block, module, require: (str(block), str(module)),
    )

    def fake_run_batch(**kwargs):
        progress_callback = kwargs["progress_callback"]
        progress_callback("workflow", 0, 2, None)
        progress_callback(
            "workflow",
            2,
            2,
            {"id": "01A0002", "status": "completed"},
        )
        progress_callback("cover", 0, 3, None)
        progress_callback(
            "cover",
            3,
            3,
            {"id": "01A0003", "status": "downloaded"},
        )
        return {
            "scope": {"block": "A", "module": "01"},
            "requested": 2,
            "processed": 2,
            "items": [],
            "cover_sweep": {"requested": 3, "processed": 3, "items": []},
            "stopped": False,
        }

    monkeypatch.setattr(workflow_jobs.workflow, "run_batch", fake_run_batch)

    job = workflow_jobs.start_job(
        block="A",
        module="01",
        limit=2,
        start_stage="metadata",
        download_cover_after_metadata=True,
    )
    deadline = time.monotonic() + 2
    final = workflow_jobs.get_job(str(job["job_id"]))
    while final["status"] not in {"completed", "stopped", "failed"}:
        assert time.monotonic() < deadline
        time.sleep(0.01)
        final = workflow_jobs.get_job(str(job["job_id"]))

    assert final["status"] == "completed"
    assert final["progress"]["workflow"]["processed"] == 2
    assert final["progress"]["workflow"]["requested"] == 2
    assert final["progress"]["cover"]["processed"] == 3
    assert final["progress"]["cover"]["requested"] == 3
