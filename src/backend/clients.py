from typing import Any

import requests

from ..llm_models import get_openai_model_profile
from .config import (
    LLM_TIMEOUT_SECONDS,
    OLLAMA_BASE_URL,
    OLLAMA_TIMEOUT_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
)


class ClientError(RuntimeError):
    """Raised when external model providers fail."""


class ClientTimeoutError(ClientError):
    """Raised when one model request exceeds its configured timeout."""


def _normalize_base_url(base_url: str | None = None) -> str:
    raw = str(base_url or OLLAMA_BASE_URL).strip()
    return raw.rstrip("/")


def _extract_ollama_error(response: requests.Response) -> str:
    detail = ""
    try:
        payload = response.json()
    except Exception:
        payload = None

    if isinstance(payload, dict):
        detail = str(payload.get("error") or payload.get("message") or "").strip()

    if not detail:
        detail = str(response.text or "").strip()

    return detail[:500]


def _ollama_post_json(
    *,
    url: str,
    body: dict[str, Any],
    timeout: float | None,
    operation: str,
) -> dict[str, Any]:
    try:
        response = requests.post(url, json=body, timeout=timeout)
    except requests.Timeout as exc:
        raise ClientTimeoutError(
            f"{operation} timed out after {timeout} seconds"
        ) from exc
    except Exception as exc:
        raise ClientError(f"{operation} request failed: {exc}") from exc

    if response.status_code >= 400:
        detail = _extract_ollama_error(response)
        suffix = f": {detail}" if detail else ""
        raise ClientError(
            f"{operation} failed ({response.status_code}) for {url}{suffix}"
        )

    try:
        payload = response.json()
    except Exception as exc:
        raise ClientError(f"{operation} returned invalid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ClientError(f"{operation} returned non-object payload")

    return payload


def _ollama_parse_chat_content(payload: dict[str, Any]) -> str:
    message = payload.get("message") if isinstance(payload, dict) else {}
    content = message.get("content") if isinstance(message, dict) else None
    return str(content or "").strip()


def _ollama_parse_generate_content(payload: dict[str, Any]) -> str:
    return str(payload.get("response") or "").strip()


def list_ollama_models(
    *, base_url: str | None = None, timeout: float = REQUEST_TIMEOUT_SECONDS
) -> list[str]:
    url = f"{_normalize_base_url(base_url)}/api/tags"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        raise ClientError(f"Failed to list Ollama models: {exc}") from exc

    models: list[str] = []
    for item in payload.get("models", []) if isinstance(payload, dict) else []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("model") or "").strip()
        if name:
            models.append(name)

    return sorted(set(models))


def ollama_chat_with_images(
    *,
    model: str,
    prompt: str,
    images_base64: list[str],
    base_url: str | None = None,
    timeout: float | None = OLLAMA_TIMEOUT_SECONDS,
) -> str:
    base = _normalize_base_url(base_url)
    chat_body = {
        "model": model,
        "stream": False,
        "messages": [
            {
                "role": "user",
                "content": prompt,
                "images": images_base64,
            }
        ],
    }
    generate_body = {
        "model": model,
        "stream": False,
        "prompt": prompt,
        "images": images_base64,
    }

    chat_url = f"{base}/api/chat"
    chat_error: ClientError | None = None
    try:
        payload = _ollama_post_json(
            url=chat_url,
            body=chat_body,
            timeout=timeout,
            operation="Ollama /api/chat",
        )
        text = _ollama_parse_chat_content(payload)
        if text:
            return text
        chat_error = ClientError(
            f"Ollama /api/chat returned empty content for {chat_url}"
        )
    except ClientTimeoutError:
        raise
    except ClientError as exc:
        chat_error = exc

    generate_url = f"{base}/api/generate"
    try:
        payload = _ollama_post_json(
            url=generate_url,
            body=generate_body,
            timeout=timeout,
            operation="Ollama /api/generate",
        )
        text = _ollama_parse_generate_content(payload)
        if text:
            return text
        raise ClientError(
            f"Ollama /api/generate returned empty response for {generate_url}"
        )
    except ClientTimeoutError:
        raise
    except ClientError as exc:
        if chat_error:
            raise ClientError(f"{chat_error}; fallback failed: {exc}") from exc
        raise


def ollama_chat_text(
    *,
    model: str,
    prompt: str,
    base_url: str | None = None,
    timeout: float | None = OLLAMA_TIMEOUT_SECONDS,
) -> str:
    base = _normalize_base_url(base_url)
    chat_body = {
        "model": model,
        "stream": False,
        "messages": [
            {
                "role": "user",
                "content": prompt,
            }
        ],
    }
    generate_body = {
        "model": model,
        "stream": False,
        "prompt": prompt,
    }

    chat_url = f"{base}/api/chat"
    chat_error: ClientError | None = None
    try:
        payload = _ollama_post_json(
            url=chat_url,
            body=chat_body,
            timeout=timeout,
            operation="Ollama /api/chat",
        )
        text = _ollama_parse_chat_content(payload)
        if text:
            return text
        chat_error = ClientError(
            f"Ollama /api/chat returned empty content for {chat_url}"
        )
    except ClientTimeoutError:
        raise
    except ClientError as exc:
        chat_error = exc

    generate_url = f"{base}/api/generate"
    try:
        payload = _ollama_post_json(
            url=generate_url,
            body=generate_body,
            timeout=timeout,
            operation="Ollama /api/generate",
        )
        text = _ollama_parse_generate_content(payload)
        if text:
            return text
        raise ClientError(
            f"Ollama /api/generate returned empty response for {generate_url}"
        )
    except ClientTimeoutError:
        raise
    except ClientError as exc:
        if chat_error:
            raise ClientError(f"{chat_error}; fallback failed: {exc}") from exc
        raise


def openai_vision_chat(
    *,
    api_key: str,
    model: str,
    prompt: str,
    image_data_url: str,
    timeout: float = LLM_TIMEOUT_SECONDS,
) -> str:
    from openai import APITimeoutError, OpenAI

    client = OpenAI(api_key=api_key, timeout=timeout)
    profile = get_openai_model_profile(model)

    try:
        if profile.endpoint == "responses":
            request: dict[str, Any] = {
                "model": model,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": prompt},
                            {
                                "type": "input_image",
                                "image_url": image_data_url,
                                "detail": profile.vision_detail,
                            },
                        ],
                    }
                ],
                "store": False,
                "max_output_tokens": 5000,
            }
            if profile.reasoning_effort is not None:
                request["reasoning"] = {"effort": profile.reasoning_effort}
            if profile.temperature is not None:
                request["temperature"] = profile.temperature
            response = client.responses.create(**request)
            output = str(response.output_text or "").strip()
        else:
            request = {
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": image_data_url,
                                    "detail": profile.vision_detail,
                                },
                            },
                        ],
                    }
                ],
            }
            if profile.temperature is not None:
                request["temperature"] = profile.temperature
            response = client.chat.completions.create(**request)
            output = str(response.choices[0].message.content or "").strip()
    except APITimeoutError as exc:
        raise ClientTimeoutError(
            f"OpenAI vision timed out after {timeout} seconds"
        ) from exc
    except Exception as exc:
        raise ClientError(f"OpenAI vision call failed: {exc}") from exc

    if not output:
        raise ClientError("OpenAI vision returned empty content")
    return output


def openai_text_chat(
    *,
    api_key: str,
    model: str,
    prompt: str,
    instructions: str | None = None,
    response_schema: dict[str, Any] | None = None,
    timeout: float = LLM_TIMEOUT_SECONDS,
) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=api_key, timeout=timeout)
    profile = get_openai_model_profile(model)

    try:
        if profile.endpoint == "responses":
            request: dict[str, Any] = {
                "model": model,
                "input": prompt,
                "store": False,
            }
            if instructions:
                request["instructions"] = instructions
            if profile.reasoning_effort is not None:
                request["reasoning"] = {"effort": profile.reasoning_effort}
            if profile.temperature is not None:
                request["temperature"] = profile.temperature
            if response_schema is not None and profile.structured_outputs:
                request["text"] = {
                    "format": {
                        "type": "json_schema",
                        "name": "book_catalog",
                        "strict": True,
                        "schema": response_schema,
                    }
                }
            response = client.responses.create(**request)
            text = str(response.output_text or "").strip()
        else:
            messages: list[dict[str, str]] = []
            if instructions:
                messages.append({"role": "system", "content": instructions})
            messages.append({"role": "user", "content": prompt})
            request = {"model": model, "messages": messages}
            if profile.temperature is not None:
                request["temperature"] = profile.temperature
            response = client.chat.completions.create(**request)
            text = str(response.choices[0].message.content or "").strip()
    except Exception as exc:
        raise ClientError(f"OpenAI text call failed: {exc}") from exc

    if not text:
        raise ClientError("OpenAI text returned empty content")
    return text
