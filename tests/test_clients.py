import base64
from pathlib import Path
from types import SimpleNamespace

import openai
import pytest
import requests

from src.backend import clients
from src.backend.services import ocr
from src.llm_models import (
    DEFAULT_OPENAI_CATALOG_MODEL,
    DEFAULT_OPENAI_OCR_MODEL,
    OPENAI_CATALOG_MODEL_OPTIONS,
    OPENAI_OCR_MODEL_OPTIONS,
    get_openai_model_profile,
)


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
    assert captured["timeout"] == 60
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
    assert traces[-1]["error_code"] == "ocr_timeout"
    assert "timed out" in traces[-1]["error"]


def test_ocr_timeout_result_includes_manual_crop_review_reason(tmp_path, monkeypatch):
    image_path = tmp_path / "page.jpg"
    image_path.write_bytes(b"image")
    updates = []

    monkeypatch.setattr(
        ocr.books,
        "get_book",
        lambda _book_id: {
            "id": "01A0001",
            "ocr_status": "pending",
            "image_paths": [str(image_path)],
        },
    )
    monkeypatch.setattr(
        ocr,
        "_ocr_with_model",
        lambda *args, **kwargs: (
            "",
            [
                {
                    "status": "error",
                    "error_code": "ocr_timeout",
                    "image": str(image_path),
                    "error": "Ollama timed out after 60 seconds",
                }
            ],
        ),
    )
    monkeypatch.setattr(
        ocr.books,
        "update_ocr",
        lambda book_id, **kwargs: updates.append((book_id, kwargs)),
    )

    result = ocr.run_one("01A0001", timeout=60)

    assert result["status"] == "error"
    assert result["error_code"] == "ocr_timeout"
    assert result["timeout_image"] == "page.jpg"
    assert "la imagen «page.jpg»" in result["review_reason"]
    assert "Recorta manualmente" in result["review_reason"]
    assert updates[-1][1]["status"] == "error"
    assert updates[-1][1]["trace"]["timeout_seconds_per_image"] == 60


def test_luna_is_default_and_sol_remains_selectable():
    assert DEFAULT_OPENAI_CATALOG_MODEL == "gpt-6-luna"
    assert OPENAI_CATALOG_MODEL_OPTIONS[0] == "gpt-6-luna"
    assert "gpt-6.1-sol" in OPENAI_CATALOG_MODEL_OPTIONS
    assert DEFAULT_OPENAI_OCR_MODEL == "gpt-6-luna"
    assert OPENAI_OCR_MODEL_OPTIONS[0] == "gpt-6-luna"
    assert "gpt-6.1-sol" in OPENAI_OCR_MODEL_OPTIONS


@pytest.mark.parametrize("model", ["gpt-6-luna", "gpt-6.1-sol"])
def test_recommended_openai_profiles_use_low_reasoning_without_temperature(model):
    profile = get_openai_model_profile(model)

    assert profile.endpoint == "responses"
    assert profile.reasoning_effort == "low"
    assert profile.temperature is None
    assert profile.structured_outputs is True


def test_openai_vision_uses_responses_with_original_detail(monkeypatch):
    captured = {}
    constructor = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(output_text="Texto de la página")

    fake_client = SimpleNamespace(responses=FakeResponses())

    def fake_openai(**kwargs):
        constructor.update(kwargs)
        return fake_client

    monkeypatch.setattr(openai, "OpenAI", fake_openai)

    result = clients.openai_vision_chat(
        api_key="test-key",
        model="gpt-6-luna",
        prompt="Transcribe",
        image_data_url="data:image/jpeg;base64,YWJj",
        timeout=60,
    )

    assert result == "Texto de la página"
    assert constructor == {"api_key": "test-key", "timeout": 60}
    assert captured["model"] == "gpt-6-luna"
    assert captured["reasoning"] == {"effort": "low"}
    assert captured["store"] is False
    assert captured["max_output_tokens"] == 5000
    assert "temperature" not in captured
    image_input = captured["input"][0]["content"][1]
    assert image_input == {
        "type": "input_image",
        "image_url": "data:image/jpeg;base64,YWJj",
        "detail": "original",
    }


def test_openai_ocr_uses_openai_for_image_and_isbn(tmp_path, monkeypatch):
    image_path = tmp_path / "page.jpg"
    image_path.write_bytes(b"jpeg-data")
    updates = []
    calls = []

    monkeypatch.setattr(ocr, "OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        ocr.books,
        "get_book",
        lambda _book_id: {
            "id": "01A0001",
            "ocr_status": "pending",
            "image_paths": [str(image_path)],
        },
    )
    monkeypatch.setattr(
        ocr,
        "_ocr_with_model",
        lambda model, paths, **kwargs: calls.append(("ocr", model, paths, kwargs))
        or ("Texto con ISBN 9788412280043", [{"status": "ok"}]),
    )
    monkeypatch.setattr(
        ocr,
        "_extract_isbn_with_llm",
        lambda text, **kwargs: calls.append(("isbn", text, kwargs))
        or {
            "isbns": ["9788412280043"],
            "isbn_raw": "9788412280043",
            "isbn": "9788412280043",
            "source": "llm_isbn_extraction",
        },
    )
    monkeypatch.setattr(
        ocr.books,
        "update_ocr",
        lambda book_id, **kwargs: updates.append((book_id, kwargs)),
    )

    result = ocr.run_one(
        "01A0001",
        provider="openai",
        model="gpt-6-luna",
        timeout=60,
    )

    assert result["status"] == "processed"
    assert result["provider"] == "openai"
    assert result["model"] == "gpt-6-luna"
    assert calls[0][3]["provider"] == "openai"
    assert calls[1][2]["provider"] == "openai"
    assert calls[1][2]["model"] == "gpt-6-luna"
    assert updates[-1][1]["provider"] == "openai"
    assert updates[-1][1]["trace"]["provider"] == "openai"


@pytest.mark.parametrize("model", ["gpt-6-luna", "gpt-6.1-sol"])
def test_openai_text_chat_uses_responses_and_structured_output(monkeypatch, model):
    captured = {}

    class FakeResponses:
        def create(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(output_text='{"titulo": "Libro"}')

    fake_client = SimpleNamespace(responses=FakeResponses())
    monkeypatch.setattr(openai, "OpenAI", lambda **_kwargs: fake_client)
    schema = {
        "type": "object",
        "properties": {"titulo": {"type": ["string", "null"]}},
        "required": ["titulo"],
        "additionalProperties": False,
    }

    result = clients.openai_text_chat(
        api_key="test-key",
        model=model,
        instructions="System instructions",
        prompt="Book sources",
        response_schema=schema,
    )

    assert result == '{"titulo": "Libro"}'
    assert captured["model"] == model
    assert captured["instructions"] == "System instructions"
    assert captured["input"] == "Book sources"
    assert captured["reasoning"] == {"effort": "low"}
    assert "temperature" not in captured
    assert captured["store"] is False
    assert captured["text"]["format"] == {
        "type": "json_schema",
        "name": "book_catalog",
        "strict": True,
        "schema": schema,
    }


def test_openai_text_chat_keeps_legacy_parameters_for_gpt_4_turbo(monkeypatch):
    captured = {}

    class FakeCompletions:
        def create(self, **kwargs):
            captured.update(kwargs)
            message = SimpleNamespace(content='{"titulo": "Libro"}')
            return SimpleNamespace(choices=[SimpleNamespace(message=message)])

    fake_client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
    monkeypatch.setattr(openai, "OpenAI", lambda **_kwargs: fake_client)

    result = clients.openai_text_chat(
        api_key="test-key",
        model="gpt-4-turbo",
        instructions="System instructions",
        prompt="Book sources",
        response_schema={"type": "object"},
    )

    assert result == '{"titulo": "Libro"}'
    assert captured == {
        "model": "gpt-4-turbo",
        "messages": [
            {"role": "system", "content": "System instructions"},
            {"role": "user", "content": "Book sources"},
        ],
        "temperature": 0.0,
    }
