import json

import pytest
import requests

from src import release_updates


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_latest_release_detects_newer_semantic_version(monkeypatch):
    captured = {}

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        return FakeResponse(
            {
                "tag_name": "v1.10.0",
                "html_url": "https://github.com/carlunio/media-catalog-books/releases/tag/v1.10.0",
            }
        )

    monkeypatch.setattr(release_updates.requests, "get", fake_get)

    status = release_updates.get_latest_release("1.2.0")

    assert status.available is True
    assert status.latest_version == "1.10.0"
    assert status.tag_name == "v1.10.0"
    assert captured["url"] == release_updates.LATEST_RELEASE_API_URL
    assert captured["timeout"] == 4.0
    assert captured["headers"]["User-Agent"] == "MC-Books/1.2.0"


@pytest.mark.parametrize("published", ["v1.2.0", "v1.1.9"])
def test_latest_release_does_not_offer_same_or_older_version(monkeypatch, published):
    monkeypatch.setattr(
        release_updates.requests,
        "get",
        lambda *_args, **_kwargs: FakeResponse({"tag_name": published}),
    )

    status = release_updates.get_latest_release("1.2.0")

    assert status.available is False
    assert status.error is None


def test_latest_release_reports_connection_errors(monkeypatch):
    monkeypatch.setattr(
        release_updates.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            requests.ConnectionError("sin conexión")
        ),
    )

    status = release_updates.get_latest_release("1.2.0")

    assert status.available is False
    assert "sin conexión" in str(status.error)


def test_request_managed_update_targets_the_active_controller(tmp_path, monkeypatch):
    runtime_path = tmp_path / "appctl.json"
    request_path = tmp_path / "update-request.json"
    runtime_path.write_text(
        json.dumps({"controller_pid": 321, "mode": "launch"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(release_updates, "RUNTIME_DIR", tmp_path)
    monkeypatch.setattr(release_updates, "RUNTIME_STATE_PATH", runtime_path)
    monkeypatch.setattr(release_updates, "UPDATE_REQUEST_PATH", request_path)

    release_updates.request_managed_update(
        current_version="1.2.0",
        target_version="1.3.0",
    )

    request = json.loads(request_path.read_text(encoding="utf-8"))
    assert request["controller_pid"] == 321
    assert request["current_version"] == "1.2.0"
    assert request["target_version"] == "1.3.0"
    assert request["version"] == 1
    assert not request_path.with_suffix(".tmp").exists()


def test_request_managed_update_requires_appctl(tmp_path, monkeypatch):
    monkeypatch.setattr(release_updates, "RUNTIME_DIR", tmp_path)
    monkeypatch.setattr(
        release_updates, "RUNTIME_STATE_PATH", tmp_path / "missing.json"
    )
    monkeypatch.setattr(
        release_updates,
        "UPDATE_REQUEST_PATH",
        tmp_path / "update-request.json",
    )

    with pytest.raises(RuntimeError, match="no está gestionada por appctl"):
        release_updates.request_managed_update(
            current_version="1.2.0",
            target_version="1.3.0",
        )
