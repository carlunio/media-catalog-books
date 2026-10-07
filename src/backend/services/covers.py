from pathlib import Path
from typing import Any

import requests

from ..config import DEFAULT_COVERS_OUTPUT_DIR, REQUEST_TIMEOUT_SECONDS
from ..normalizers import split_book_id
from . import books


def _cover_candidates(metadata: dict[str, Any]) -> list[str]:
    urls: list[str] = []

    open_library = (
        metadata.get("open_library")
        if isinstance(metadata.get("open_library"), dict)
        else {}
    )
    google = metadata.get("google") if isinstance(metadata.get("google"), dict) else {}
    isbndb = metadata.get("isbndb") if isinstance(metadata.get("isbndb"), dict) else {}

    open_library_edition = (
        open_library.get("edition")
        if isinstance(open_library.get("edition"), dict)
        else {}
    )
    for open_library_payload in (open_library_edition, open_library):
        cover = (
            open_library_payload.get("cover")
            if isinstance(open_library_payload.get("cover"), dict)
            else {}
        )
        for key in ("large", "medium", "small"):
            value = cover.get(key)
            if isinstance(value, str) and value.strip():
                urls.append(value.strip())

    image_links = (
        google.get("imageLinks") if isinstance(google.get("imageLinks"), dict) else {}
    )
    for key in ("thumbnail", "smallThumbnail", "small", "medium", "large"):
        value = image_links.get(key)
        if isinstance(value, str) and value.strip():
            urls.append(value.strip())

    isbndb_book = isbndb.get("book") if isinstance(isbndb.get("book"), dict) else {}
    image = isbndb_book.get("image")
    if isinstance(image, str) and image.strip():
        urls.append(image.strip())

    unique: list[str] = []
    seen: set[str] = set()
    for url in urls:
        key = url.lower()
        if key not in seen:
            seen.add(key)
            unique.append(url)
    return unique


def _preferred_cover_candidates(metadata: dict[str, Any]) -> list[str]:
    """Return large/medium Open Library and Google covers in provider order."""
    urls: list[str] = []

    open_library = (
        metadata.get("open_library")
        if isinstance(metadata.get("open_library"), dict)
        else {}
    )
    open_library_edition = (
        open_library.get("edition")
        if isinstance(open_library.get("edition"), dict)
        else {}
    )
    for open_library_payload in (open_library_edition, open_library):
        cover = (
            open_library_payload.get("cover")
            if isinstance(open_library_payload.get("cover"), dict)
            else {}
        )
        for key in ("large", "medium"):
            value = cover.get(key)
            if isinstance(value, str) and value.strip():
                urls.append(value.strip())

    google = metadata.get("google") if isinstance(metadata.get("google"), dict) else {}
    image_links = (
        google.get("imageLinks") if isinstance(google.get("imageLinks"), dict) else {}
    )
    for key in ("large", "medium"):
        value = image_links.get(key)
        if isinstance(value, str) and value.strip():
            urls.append(value.strip())

    unique: list[str] = []
    seen: set[str] = set()
    for url in urls:
        key = url.lower()
        if key not in seen:
            seen.add(key)
            unique.append(url)
    return unique


def _extension_from_response(response: requests.Response) -> str:
    content_type = str(response.headers.get("Content-Type") or "").lower()
    if "png" in content_type:
        return ".png"
    if "webp" in content_type:
        return ".webp"
    return ".jpg"


def _download_one(url: str, destination_base: Path, *, timeout: float) -> Path:
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    ext = _extension_from_response(response)
    path = destination_base.with_suffix(ext)

    with path.open("wb") as file:
        for chunk in response.iter_content(chunk_size=32 * 1024):
            if chunk:
                file.write(chunk)

    return path


def _output_dir_for_book(book_id: str) -> Path:
    parts = split_book_id(book_id)
    if parts is None:
        return DEFAULT_COVERS_OUTPUT_DIR
    module_value, block_value, _ = parts
    return DEFAULT_COVERS_OUTPUT_DIR / block_value / module_value


def existing_cover_file(book_id: str) -> Path | None:
    output_dir = _output_dir_for_book(book_id)
    for suffix in (".jpg", ".jpeg", ".png", ".webp"):
        candidate = output_dir / f"{book_id}{suffix}"
        if candidate.is_file():
            return candidate
    return None


def run_one(
    book_id: str, *, overwrite: bool = False, timeout: float = REQUEST_TIMEOUT_SECONDS
) -> dict[str, Any]:
    book = books.get_book(book_id)
    if book is None:
        return {"id": book_id, "status": "error", "error": "Book not found"}

    existing_cover_path = str(book.get("cover_path") or "").strip()
    existing_file = (
        Path(existing_cover_path)
        if existing_cover_path and Path(existing_cover_path).is_file()
        else existing_cover_file(book_id)
    )
    if existing_file is not None and not overwrite:
        books.update_cover(
            book_id,
            cover_path=str(existing_file.resolve()),
            status="downloaded",
            error=None,
        )
        return {
            "id": book_id,
            "status": "skipped",
            "reason": "cover already downloaded",
            "cover_path": str(existing_file.resolve()),
        }

    metadata = book.get("metadata") if isinstance(book.get("metadata"), dict) else {}
    urls = _cover_candidates(metadata)
    preferred_urls = _preferred_cover_candidates(metadata)

    if not urls:
        books.update_cover(
            book_id, cover_path=None, status="missing", error="No cover URL in metadata"
        )
        return {
            "id": book_id,
            "status": "missing",
            "reason": "No cover URL in metadata",
        }

    output_dir = _output_dir_for_book(book_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    tmp_downloads: list[Path] = []
    errors: list[str] = []

    primary_urls = preferred_urls or urls
    for idx, url in enumerate(primary_urls):
        try:
            tmp_path = _download_one(
                url, output_dir / f"{book_id}__candidate_{idx:02d}", timeout=timeout
            )
            tmp_downloads.append(tmp_path)
        except Exception as exc:
            errors.append(f"{url}: {exc}")

    # A declared large/medium URL can still be broken. Only when every preferred
    # download fails do we fall back to the remaining candidates.
    if preferred_urls and not tmp_downloads:
        preferred_set = {url.lower() for url in preferred_urls}
        fallback_urls = [url for url in urls if url.lower() not in preferred_set]
        for idx, url in enumerate(fallback_urls, start=len(primary_urls)):
            try:
                tmp_path = _download_one(
                    url,
                    output_dir / f"{book_id}__candidate_{idx:02d}",
                    timeout=timeout,
                )
                tmp_downloads.append(tmp_path)
            except Exception as exc:
                errors.append(f"{url}: {exc}")

    if not tmp_downloads:
        books.update_cover(
            book_id,
            cover_path=None,
            status="error",
            error="; ".join(errors) or "All cover downloads failed",
        )
        return {"id": book_id, "status": "error", "error": errors}

    best = max(
        tmp_downloads, key=lambda path: path.stat().st_size if path.exists() else 0
    )
    final_path = output_dir / f"{book_id}{best.suffix.lower()}"

    for suffix in (".jpg", ".jpeg", ".png", ".webp"):
        existing = output_dir / f"{book_id}{suffix}"
        if existing.exists():
            existing.unlink()
    best.rename(final_path)

    for tmp in tmp_downloads:
        if tmp != best and tmp.exists():
            tmp.unlink()

    books.update_cover(
        book_id,
        cover_path=str(final_path.resolve()),
        status="downloaded",
        error=None if not errors else f"Partial download errors: {len(errors)}",
    )

    return {
        "id": book_id,
        "status": "downloaded",
        "cover_path": str(final_path.resolve()),
        "errors": errors,
    }
