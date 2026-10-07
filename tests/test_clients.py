import base64
from pathlib import Path

import pytest
import requests

from src.backend import clients
from src.backend.services import ocr


def test_ollama_timeout_does_not_start_generate_fallback(monkeypatch):
    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        raise requests.ReadTimeout("stuck")

    monkeypatch.setattr(clients.requests, "post", fake_post)

    with pytest.raises(clients.ClientTimeoutError, match="timed out"):
        clients.ollama_chat_text(model="test-model", prompt="test", timeout=0.01)

    assert len(calls) == 1
    assert calls[0][0].endswith("/api/chat")
    assert calls[0][1]["timeout"] == 0.01


def test_ocr_image_uses_timeout_aware_http_client(tmp_path, monkeypatch):
    image_path = tmp_path / "page.jpg"
    image_bytes = b"test-image"
    image_path.write_bytes(image_bytes)
    captured = {}

    def fake_chat_with_images(**kwargs):
        captured.update(kwargs)
        return "ocr text"

    monkeypatch.setattr(ocr, "ollama_chat_with_images", fake_chat_with_images)

    result = ocr._ollama_chat_with_image(
        model="test-model", image_path=image_path, prompt="read"
    )

    assert result == "ocr text"
    assert captured["model"] == "test-model"
    assert captured["prompt"] == "read"
    assert base64.b64decode(captured["images_base64"][0]) == image_bytes


def test_ocr_timeout_stops_current_book_without_using_partial_text(monkeypatch):
    calls = []

    def fake_run_ocr_for_image(**kwargs):
        calls.append(kwargs["image_path"])
        if len(calls) == 1:
            return "partial text", {"resized": False}
        raise clients.ClientTimeoutError("Ollama timed out after 300 seconds")

    monkeypatch.setattr(ocr, "_run_ocr_for_image", fake_run_ocr_for_image)

    text, traces = ocr._ocr_with_model(
        "test-model",
        [Path("first.jpg"), Path("stuck.jpg"), Path("never-called.jpg")],
    )

    assert text == ""
    assert calls == [Path("first.jpg"), Path("stuck.jpg")]
    assert [trace["status"] for trace in traces] == ["ok", "error"]
    assert "timed out" in traces[-1]["error"]
