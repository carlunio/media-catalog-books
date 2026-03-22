from pathlib import Path
import re
import tempfile
from typing import Any

from PIL import Image

try:
    from ollama import chat as ollama_chat  # type: ignore
except Exception:  # pragma: no cover
    ollama_chat = None

from ..clients import ClientError
from ..config import OCR_ISBN_OLLAMA_MODEL, OCR_OLLAMA_MODEL
from ..normalizers import clean_isbn, is_valid_isbn
from . import books

OCR_TEXT_PROMPT = (
    "Transcribe absolutamente todo el texto que veas"
    "Respeta los saltos de línea. "
    "No inventes texto ni añadas comentarios."
)

ISBN_PROMPT = (
    "Extrae todos los ISBN presentes en el texto, si existen.\n\n"
    "Devuelve únicamente los ISBN, sin texto adicional, "
    "en una sola línea, separados por punto y coma (;).\n\n"
    "Los ISBN pueden ser ISBN-10 o ISBN-13 y pueden contener "
    "errores típicos de OCR.\n\n"
    "Puedes corregir únicamente confusiones visuales evidentes "
    "cuando formen parte de un ISBN, como:\n"
    "- I o l -> 1\n"
    "- O -> 0\n\n"
    "Ordena los ISBN por importancia, siguiendo este criterio:\n"
    "1) obra individual > obra completa\n"
    "2) editorial pequeña > editorial grande\n"
    "3) ISBN-10 > ISBN-13\n\n"
    "No inventes ISBN ni completes números faltantes. "
    "Si no hay ISBN claros, responde con una cadena vacía."
)

ISBN_CANDIDATE_PATTERN = re.compile(r"[0-9XxIiLlOo\- ]{9,}")


def _extract_ollama_content(response: Any) -> str:
    if isinstance(response, dict):
        message = response.get("message")
        if isinstance(message, dict):
            return str(message.get("content") or "").strip()
        return str(response.get("response") or "").strip()

    message = getattr(response, "message", None)
    if message is not None:
        content = getattr(message, "content", None)
        if content is not None:
            return str(content).strip()

    fallback = getattr(response, "response", None)
    if fallback is not None:
        return str(fallback).strip()

    return ""


def _normalize_ocular_isbn_confusions(text: str) -> str:
    return str(text or "").upper().replace("I", "1").replace("L", "1").replace("O", "0")


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        output.append(candidate)
    return output


def _clean_isbn_candidates(value: str) -> list[str]:
    parts = [item.strip() for item in str(value or "").split(";") if item.strip()]
    cleaned: list[str] = []
    for part in parts:
        normalized = re.sub(r"[^0-9X]", "", part.upper())
        if normalized:
            cleaned.append(normalized)
    return _unique(cleaned)


def _isbn10_valid(isbn: str) -> bool:
    if len(isbn) != 10:
        return False

    total = 0
    for index, char in enumerate(isbn):
        if char == "X":
            value = 10 if index == 9 else -1
        elif char.isdigit():
            value = int(char)
        else:
            return False

        if value < 0:
            return False

        total += value * (10 - index)

    return total % 11 == 0


def _isbn13_valid(isbn: str) -> bool:
    if len(isbn) != 13 or not isbn.isdigit():
        return False

    total = 0
    for index, char in enumerate(isbn[:12]):
        total += int(char) * (1 if index % 2 == 0 else 3)

    check = (10 - (total % 10)) % 10
    return check == int(isbn[-1])


def _isbn_valid(isbn: str) -> bool:
    return _isbn10_valid(isbn) or _isbn13_valid(isbn)


def _all_isbn_valid(candidates: list[str]) -> bool:
    if not candidates:
        return False
    return all(_isbn_valid(item) for item in candidates)


def _any_isbn_valid(candidates: list[str]) -> bool:
    if not candidates:
        return False
    return any(_isbn_valid(item) for item in candidates)


def _isbn_candidate_detail(raw_value: str) -> dict[str, Any]:
    value = str(raw_value or "").strip()
    cleaned = clean_isbn(value)
    detail: dict[str, Any] = {
        "raw": value,
        "cleaned": cleaned,
        "valid": False,
        "kind": "unknown",
        "reason": "invalid_format",
    }

    if re.fullmatch(r"\d{9}[0-9X]", cleaned):
        detail["kind"] = "isbn10"
        if _isbn10_valid(cleaned):
            detail["valid"] = True
            detail["reason"] = "valid_isbn10_checksum"
        else:
            detail["reason"] = "invalid_isbn10_checksum"
        return detail

    if re.fullmatch(r"\d{13}", cleaned):
        detail["kind"] = "isbn13"
        if _isbn13_valid(cleaned):
            detail["valid"] = True
            detail["reason"] = "valid_isbn13_checksum"
        else:
            detail["reason"] = "invalid_isbn13_checksum"
        return detail

    detail["reason"] = "invalid_length_or_chars"
    return detail


def _isbn_candidate_details(candidates: list[str]) -> list[dict[str, Any]]:
    return [_isbn_candidate_detail(value) for value in candidates]


def _ollama_chat_with_image(*, model: str, image_path: Path, prompt: str) -> str:
    if ollama_chat is None:
        raise ClientError("ollama package is not available in this environment")

    if not image_path.exists() or not image_path.is_file():
        raise ClientError(f"Image path not found: {image_path}")

    try:
        response = ollama_chat(
            model=model,
            keep_alive="5m",
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                    "images": [str(image_path)],
                }
            ],
            options={"temperature": 0.0},
        )
    except Exception as exc:
        raise ClientError(f"Ollama chat failed: {exc}") from exc

    text = _extract_ollama_content(response)
    if not text:
        raise ClientError("Ollama chat returned empty content")

    return text


def _is_glm_ocr_model(model: str | None) -> bool:
    return str(model or "").strip().lower().startswith("glm-ocr")


def _prepare_image_for_ocr(
    *,
    model: str,
    image_path: Path,
    resize_to_1800: bool,
) -> tuple[Path, dict[str, Any], Path | None]:
    preprocess: dict[str, Any] = {
        "enabled": bool(resize_to_1800),
        "model_is_glm_ocr": _is_glm_ocr_model(model),
        "resized": False,
        "original_image": str(image_path),
        "prepared_image": str(image_path),
    }

    if not resize_to_1800:
        preprocess["reason"] = "disabled"
        return image_path, preprocess, None

    if not _is_glm_ocr_model(model):
        preprocess["reason"] = "model_not_glm_ocr"
        return image_path, preprocess, None

    try:
        with Image.open(image_path) as image:
            width, height = image.size
            preprocess["original_size"] = {"width": int(width), "height": int(height)}
            max_side = max(width, height)
            if max_side <= 1800:
                preprocess["reason"] = "already_within_limit"
                return image_path, preprocess, None

            scale = 1800.0 / float(max_side)
            new_size = (
                max(1, int(round(width * scale))),
                max(1, int(round(height * scale))),
            )
            try:
                resample = Image.Resampling.LANCZOS  # type: ignore[attr-defined]
            except AttributeError:  # pragma: no cover
                resample = Image.LANCZOS  # type: ignore[attr-defined]

            resized = image.resize(new_size, resample)
            suffix = image_path.suffix.lower()
            if suffix not in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}:
                suffix = ".jpg"

            temp_file = tempfile.NamedTemporaryFile(prefix="ocr_glm_1800_", suffix=suffix, delete=False)
            temp_path = Path(temp_file.name)
            temp_file.close()

            output = resized
            if suffix in {".jpg", ".jpeg"}:
                if output.mode not in {"RGB", "L"}:
                    output = output.convert("RGB")
                output.save(temp_path, format="JPEG", quality=95, optimize=True)
            elif suffix == ".png":
                output.save(temp_path, format="PNG")
            elif suffix == ".webp":
                if output.mode not in {"RGB", "L"}:
                    output = output.convert("RGB")
                output.save(temp_path, format="WEBP", quality=95)
            elif suffix in {".tif", ".tiff"}:
                output.save(temp_path, format="TIFF")
            elif suffix == ".bmp":
                if output.mode not in {"RGB", "L"}:
                    output = output.convert("RGB")
                output.save(temp_path, format="BMP")
            else:
                if output.mode not in {"RGB", "L"}:
                    output = output.convert("RGB")
                output.save(temp_path, format="JPEG", quality=95, optimize=True)

            preprocess["resized"] = True
            preprocess["reason"] = "resized_for_glm_ocr"
            preprocess["prepared_image"] = str(temp_path)
            preprocess["prepared_size"] = {"width": int(new_size[0]), "height": int(new_size[1])}
            preprocess["max_side_limit"] = 1800
            return temp_path, preprocess, temp_path
    except Exception as exc:
        preprocess["reason"] = "preprocess_failed"
        preprocess["error"] = str(exc)
        return image_path, preprocess, None


def _run_ocr_for_image(
    *,
    model: str,
    image_path: Path,
    resize_to_1800: bool = False,
) -> tuple[str, dict[str, Any]]:
    prepared_path, preprocess, temp_path = _prepare_image_for_ocr(
        model=model,
        image_path=image_path,
        resize_to_1800=resize_to_1800,
    )

    try:
        text = _ollama_chat_with_image(model=model, image_path=prepared_path, prompt=OCR_TEXT_PROMPT)
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

    cleaned = text.strip()
    if not cleaned:
        raise ClientError("Provider returned empty OCR text")
    return cleaned, preprocess


def _extract_isbn_with_llm(credits_text: str, *, model: str) -> dict[str, Any]:
    text = str(credits_text or "").strip()
    if not text:
        return {
            "raw_response": "",
            "normalized_response": "",
            "isbns": [],
            "isbn_raw": None,
            "isbn": None,
            "is_valid": False,
            "todos_validos": False,
            "alguno_valido": False,
            "source": "empty_ocr_text",
        }

    if ollama_chat is None:
        raise ClientError("ollama package is not available in this environment")

    try:
        response = ollama_chat(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": f"{ISBN_PROMPT}\n\nTEXTO:\n{text}",
                }
            ],
            options={"temperature": 0.0},
        )
    except Exception as exc:
        raise ClientError(f"Ollama ISBN chat failed: {exc}") from exc

    raw = _extract_ollama_content(response)
    normalized = _normalize_ocular_isbn_confusions(raw)
    candidates = _clean_isbn_candidates(normalized)
    candidate_details = _isbn_candidate_details(candidates)

    valid_candidates = [item for item in candidates if _isbn_valid(item)]
    selected = valid_candidates[0] if valid_candidates else None

    return {
        "raw_response": raw,
        "normalized_response": normalized,
        "isbns": candidates,
        "candidate_details": candidate_details,
        "valid_candidates": valid_candidates,
        "isbn_raw": candidates[0] if candidates else None,
        "isbn": selected,
        "is_valid": bool(selected),
        "todos_validos": _all_isbn_valid(candidates),
        "alguno_valido": _any_isbn_valid(candidates),
        "source": "llm_isbn_extraction",
    }


def _ocr_with_model(
    model: str,
    image_paths: list[Path],
    *,
    resize_to_1800: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    traces: list[dict[str, Any]] = []
    chunks: list[str] = []

    for index, path in enumerate(image_paths[:4], start=1):
        attempt: dict[str, Any] = {
            "provider": "ollama",
            "model": model,
            "image": str(path),
            "index": index,
        }

        try:
            text, preprocess = _run_ocr_for_image(
                model=model,
                image_path=path,
                resize_to_1800=resize_to_1800,
            )
            attempt["preprocess"] = preprocess
            if text:
                chunks.append(text)
                attempt["status"] = "ok"
                attempt["chars"] = len(text)
                attempt["sample"] = text[:220]
            else:
                attempt["status"] = "invalid"
                attempt["chars"] = 0
                attempt["error"] = "Provider returned empty OCR text"
        except Exception as exc:
            attempt["status"] = "error"
            attempt["chars"] = 0
            attempt["error"] = str(exc)

        traces.append(attempt)

    return "\n".join(chunks).strip(), traces


def _compact_ocr_attempts(traces: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(traces)
    ok = 0
    invalid = 0
    error = 0
    first_error: str | None = None

    for attempt in traces:
        status = str(attempt.get("status") or "").strip().lower()
        if status == "ok":
            ok += 1
        elif status == "invalid":
            invalid += 1
        elif status == "error":
            error += 1

        if first_error is None:
            detail = str(attempt.get("error") or "").strip()
            if detail:
                first_error = detail[:320]

    return {
        "total": total,
        "ok": ok,
        "invalid": invalid,
        "error": error,
        "first_error": first_error,
    }


def _compact_isbn_extraction(
    *,
    provider: str,
    model: str,
    isbn_data: dict[str, Any],
    isbn_error: str | None,
) -> dict[str, Any]:
    candidates = isbn_data.get("isbns") if isinstance(isbn_data.get("isbns"), list) else []
    compact_candidates = [str(item) for item in candidates[:5] if str(item).strip()]

    return {
        "provider": provider,
        "model": model,
        "source": str(isbn_data.get("source") or "").strip() or None,
        "isbn_raw": isbn_data.get("isbn_raw"),
        "isbn": isbn_data.get("isbn"),
        "is_valid": bool(isbn_data.get("isbn")),
        "candidates": compact_candidates,
        "candidates_count": len(candidates),
        "error": str(isbn_error or "").strip() or None,
    }


def derive_isbn_from_text(credits_text: str | None) -> dict[str, Any]:
    text = str(credits_text or "")

    raw_candidates = [clean_isbn(match) for match in ISBN_CANDIDATE_PATTERN.findall(text)]
    raw_candidates = [item for item in raw_candidates if item]
    raw_candidates = _unique(raw_candidates)

    normalized_candidates = _unique([_normalize_ocular_isbn_confusions(item) for item in raw_candidates])
    valid_candidates = _unique([item for item in raw_candidates if is_valid_isbn(item)])
    normalized_valid_candidates = _unique([item for item in normalized_candidates if is_valid_isbn(item)])
    candidate_details = _isbn_candidate_details(raw_candidates)
    normalized_candidate_details = _isbn_candidate_details(normalized_candidates)

    selected = None
    source = None
    if valid_candidates:
        selected = valid_candidates[0]
        source = "checksum_direct"
    elif normalized_valid_candidates:
        selected = normalized_valid_candidates[0]
        source = "checksum_after_ocr_normalization"

    return {
        "isbn_raw": raw_candidates[0] if raw_candidates else None,
        "isbn": selected,
        "is_valid": bool(selected),
        "source": source,
        "rules": [
            "regex candidate extraction",
            "ISBN-10 checksum validation",
            "ISBN-13 checksum validation",
            "OCR normalization (I/L->1, O->0) before revalidation",
        ],
        "raw_candidates": raw_candidates,
        "normalized_candidates": normalized_candidates,
        "valid_candidates": valid_candidates,
        "normalized_valid_candidates": normalized_valid_candidates,
        "candidate_details": candidate_details,
        "normalized_candidate_details": normalized_candidate_details,
    }


def run_one(
    book_id: str,
    *,
    provider: str | None = None,
    model: str | None = None,
    resize_to_1800: bool = False,
    overwrite: bool = False,
) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    existing_status = str(book.get("ocr_status") or "").strip().lower()
    if existing_status in {"processed", "manual"} and not overwrite:
        return {"id": book_id, "status": "skipped", "reason": "ocr already present"}

    image_paths_raw = [str(path).strip() for path in book.get("image_paths", []) if str(path).strip()]
    image_paths = [Path(path) for path in image_paths_raw if Path(path).exists() and Path(path).is_file()]

    if not image_paths:
        fallback = books.ensure_local_image_path(book_id)
        if fallback:
            fallback_path = Path(fallback)
            if fallback_path.exists() and fallback_path.is_file():
                image_paths = [fallback_path]

    if not image_paths:
        books.update_ocr(
            book_id,
            credits_text=None,
            isbn_raw=None,
            isbn=None,
            status="error",
            provider=None,
            model=None,
            trace=[{"status": "error", "reason": "No image file available"}],
            error="No image file available",
        )
        return {"id": book_id, "status": "error", "error": "No image file available"}

    requested_provider = str(provider or "ollama").strip().lower() or "ollama"
    selected_model = str(model or OCR_OLLAMA_MODEL).strip() or OCR_OLLAMA_MODEL
    selected_isbn_model = str(OCR_ISBN_OLLAMA_MODEL).strip() or "gpt-oss:20b"

    credits_text, traces = _ocr_with_model(
        selected_model,
        image_paths,
        resize_to_1800=bool(resize_to_1800),
    )

    if not credits_text:
        message = "All OCR provider attempts failed"
        for attempt in traces:
            detail = str(attempt.get("error") or "").strip()
            if detail:
                message = f"{message}: {detail[:320]}"
                break

        compact_attempts = _compact_ocr_attempts(traces)
        trace_payload = {
            "source": "provider",
            "provider_requested": requested_provider,
            "provider": "ollama",
            "model": selected_model,
            "resize_to_1800_requested": bool(resize_to_1800),
            "resize_to_1800_applied": bool(resize_to_1800 and _is_glm_ocr_model(selected_model)),
            "ocr_attempts": compact_attempts,
        }

        books.update_ocr(
            book_id,
            credits_text=None,
            isbn_raw=None,
            isbn=None,
            status="error",
            provider="ollama",
            model=selected_model,
            trace=trace_payload,
            error=message,
        )
        return {"id": book_id, "status": "error", "error": message, "trace": traces}

    isbn_data: dict[str, Any]
    isbn_error: str | None = None
    try:
        isbn_data = _extract_isbn_with_llm(credits_text, model=selected_isbn_model)
    except Exception as exc:
        isbn_error = str(exc)
        fallback = derive_isbn_from_text(credits_text)
        isbn_data = {
            "raw_response": "",
            "normalized_response": "",
            "isbns": fallback.get("raw_candidates", []),
            "isbn_raw": fallback.get("isbn_raw"),
            "isbn": fallback.get("isbn"),
            "is_valid": bool(fallback.get("isbn")),
            "todos_validos": False,
            "alguno_valido": bool(fallback.get("isbn")),
            "source": "regex_fallback_after_llm_error",
        }

    isbn_raw_value = isbn_data.get("isbn_raw")
    isbn_value = isbn_data.get("isbn")

    compact_attempts = _compact_ocr_attempts(traces)
    compact_isbn = _compact_isbn_extraction(
        provider="ollama",
        model=selected_isbn_model,
        isbn_data=isbn_data,
        isbn_error=isbn_error,
    )
    trace_payload = {
        "source": "provider",
        "provider_requested": requested_provider,
        "provider": "ollama",
        "model": selected_model,
        "resize_to_1800_requested": bool(resize_to_1800),
        "resize_to_1800_applied": bool(resize_to_1800 and _is_glm_ocr_model(selected_model)),
        "ocr_attempts": compact_attempts,
        "isbn_extraction": compact_isbn,
    }

    books.update_ocr(
        book_id,
        credits_text=credits_text,
        isbn_raw=isbn_raw_value,
        isbn=isbn_value,
        status="processed",
        provider="ollama",
        model=selected_model,
        trace=trace_payload,
        error=None,
    )

    return {
        "id": book_id,
        "status": "processed",
        "isbn": isbn_value,
        "isbn_raw": isbn_raw_value,
        "provider": "ollama",
        "model": selected_model,
        "isbn_model": selected_isbn_model,
        "resize_to_1800_requested": bool(resize_to_1800),
        "resize_to_1800_applied": bool(resize_to_1800 and _is_glm_ocr_model(selected_model)),
        "chars": len(credits_text),
        "isbn_valid": bool(isbn_value),
    }
