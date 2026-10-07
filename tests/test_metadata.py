import traceback

import pytest
import requests

from src.backend.services import books, metadata


def test_google_books_requires_and_uses_a_project_api_key(monkeypatch):
    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_API_KEY", None)
    with pytest.raises(RuntimeError, match="GOOGLE_BOOKS_API_KEY"):
        metadata._google_books("9788412280043", timeout=5)

    calls = []

    def fake_safe_get(url, **kwargs):
        calls.append((url, kwargs))
        return {"items": [{"volumeInfo": {"title": "Libro de prueba"}}]}

    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_API_KEY", "private-test-key")
    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(metadata, "_safe_get", fake_safe_get)

    result = metadata._google_books("9788412280043", timeout=5)

    assert result == {
        "title": "Libro de prueba",
        "lookup_method": "isbn",
    }
    assert calls == [
        (
            "https://www.googleapis.com/books/v1/volumes",
            {
                "headers": {"X-Goog-Api-Key": "private-test-key"},
                "params": {
                    "q": "isbn:9788412280043",
                    "maxResults": 1,
                },
                "timeout": 5,
            },
        )
    ]


def test_google_books_retries_transient_http_errors(monkeypatch):
    calls = []
    sleeps = []

    class RetryableResponse:
        status_code = 503

    def fake_safe_get(url, **kwargs):
        calls.append((url, kwargs))
        if len(calls) == 1:
            raise requests.HTTPError(
                "HTTP 503: Service temporarily unavailable",
                response=RetryableResponse(),
            )
        return {"items": [{"volumeInfo": {"title": "Libro recuperado"}}]}

    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_API_KEY", "private-test-key")
    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(metadata, "_safe_get", fake_safe_get)
    monkeypatch.setattr(metadata.time, "sleep", sleeps.append)

    result = metadata._google_books("9788412280043", timeout=5)

    assert result == {
        "title": "Libro recuperado",
        "lookup_method": "isbn",
    }
    assert len(calls) == 2
    assert sleeps == [1]


def test_google_lookup_method_is_persisted_with_provider_payload(monkeypatch):
    saved = {}

    monkeypatch.setattr(
        metadata.books,
        "get_book",
        lambda _book_id: {
            "id": "01A0001",
            "isbn": "9788412280043",
            "metadata_status": "pending",
        },
    )
    monkeypatch.setattr(metadata, "_open_library", lambda *args, **kwargs: {})
    monkeypatch.setattr(metadata, "_isbndb", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        metadata,
        "_google_books",
        lambda *args, **kwargs: {
            "title": "Libro de prueba",
            "lookup_method": "isbn",
        },
    )
    monkeypatch.setattr(
        metadata.books,
        "update_metadata",
        lambda book_id, **kwargs: saved.update({"id": book_id, **kwargs}),
    )

    result = metadata.run_one("01A0001")

    assert result["google_status"] == "fetched"
    assert saved["metadata"]["google"]["lookup_method"] == "isbn"


def test_metadata_rows_expose_each_provider_status():
    result = books._metadata_from_rows(
        "01A0001",
        [
            ("google", "{}", "9788412280043", "empty", None, "2026-10-04T10:00:00Z"),
            (
                "openlibrary",
                '{"title":"Ficha OL"}',
                "9788412280043",
                "fetched",
                None,
                "2026-10-04T10:00:00Z",
            ),
            (
                "isbndb",
                '{"book":{"title":"Ficha ISBNdb"}}',
                "9788412280043",
                "fetched",
                None,
                "2026-10-04T10:00:00Z",
            ),
        ],
    )

    assert result["provider_statuses"] == {
        "google": "empty",
        "open_library": "fetched",
        "isbndb": "fetched",
    }
    assert result["google"] == {}
    assert result["open_library"]["title"] == "Ficha OL"
    assert result["isbndb"]["book"]["title"] == "Ficha ISBNdb"


def test_open_library_fetches_exact_edition_and_small_work_context(monkeypatch):
    calls = []

    def fake_safe_get(url, **kwargs):
        calls.append((url, kwargs))
        if "/isbn/" in url:
            return {
                "key": "/books/OL123M",
                "title": "Por qué nos creemos los cuentos",
                "publishers": ["Clave Intelectual"],
                "publish_date": "2021",
                "number_of_pages": 160,
                "physical_format": "Paperback",
                "isbn_10": ["8412280040"],
                "isbn_13": ["9788412280043"],
                "covers": [13686269],
                "works": [{"key": "/works/OL34886673W"}],
                "revision": 7,
            }
        return {
            "docs": [
                {
                    "key": "/works/OL34886673W",
                    "title": "Por qué nos creemos los cuentos",
                    "author_name": ["Pablo Maurette"],
                    "author_key": ["OL7648519A"],
                    "first_publish_year": 2021,
                    "subject": ["Narración", "Literatura"],
                    "description": "Una historia sobre el poder de los relatos.",
                }
            ]
        }

    monkeypatch.setattr(metadata, "OPENLIBRARY_CONTACT", "catalog@example.test")
    monkeypatch.setattr(metadata, "OPENLIBRARY_MIN_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(metadata, "_safe_get", fake_safe_get)

    result = metadata._open_library("9788412280043", timeout=5)

    assert [url for url, _kwargs in calls] == [
        "https://openlibrary.org/isbn/9788412280043.json",
        "https://openlibrary.org/search.json",
    ]
    search_kwargs = calls[1][1]
    assert search_kwargs["params"]["isbn"] == "9788412280043"
    assert search_kwargs["params"]["email"] == "catalog@example.test"
    assert search_kwargs["headers"] == {
        "User-Agent": "media-catalog-books/1.0 (catalog@example.test)"
    }

    assert result["requested_isbn"] == "9788412280043"
    edition = result["edition"]
    assert edition["publishers"] == ["Clave Intelectual"]
    assert edition["publish_date"] == "2021"
    assert edition["number_of_pages"] == 160
    assert edition["identifiers"] == {
        "isbn_10": ["8412280040"],
        "isbn_13": ["9788412280043"],
    }
    assert edition["cover"]["large"].endswith("/13686269-L.jpg")
    assert edition["url"] == "https://openlibrary.org/books/OL123M"

    work = result["work"]
    assert work["authors"] == [
        {
            "name": "Pablo Maurette",
            "url": "https://openlibrary.org/authors/OL7648519A",
        }
    ]
    assert work["subject"] == ["Narración", "Literatura"]
    assert work["description"] == "Una historia sobre el poder de los relatos."
    assert work["url"] == "https://openlibrary.org/works/OL34886673W"


def test_google_title_fallback_requires_matching_isbn(monkeypatch):
    calls = []

    def fake_safe_get(url, **kwargs):
        calls.append(kwargs["params"])
        query = kwargs["params"]["q"]
        if query.startswith("isbn:"):
            return {}
        if query == "Título recuperable":
            return {
                "items": [
                    {
                        "id": "google-volume-id",
                        "volumeInfo": {
                            "title": "Título recuperable",
                            "industryIdentifiers": [
                                {
                                    "type": "ISBN_10",
                                    "identifier": "8412280040",
                                }
                            ],
                        },
                    }
                ]
            }
        raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_API_KEY", "private-test-key")
    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(metadata, "_safe_get", fake_safe_get)

    result = metadata._google_books(
        "9788412280043",
        timeout=5,
        title_candidates=["Título recuperable"],
    )

    assert result["title"] == "Título recuperable"
    assert result["google_volume_id"] == "google-volume-id"
    assert result["lookup_method"] == "title_validated_by_isbn"
    assert [call["q"] for call in calls] == [
        "isbn:9788412280043",
        "Título recuperable",
    ]


def test_google_false_empty_field_search_is_reported_as_unavailable(monkeypatch):
    def fake_safe_get(url, **kwargs):
        query = kwargs["params"]["q"]
        if query == "flowers":
            return {"items": [{"volumeInfo": {"title": "Flowers"}}]}
        return {}

    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_API_KEY", "private-test-key")
    monkeypatch.setattr(metadata, "GOOGLE_BOOKS_MIN_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(metadata, "_google_field_search_health", None)
    monkeypatch.setattr(metadata, "_safe_get", fake_safe_get)

    with pytest.raises(
        metadata.GoogleFieldSearchUnavailable,
        match="field search is unavailable",
    ):
        metadata._google_books("9788412280043", timeout=5)


def test_http_error_message_does_not_include_query_credentials(monkeypatch):
    class FakeResponse:
        status_code = 429
        reason = "Too Many Requests"

        @staticmethod
        def json():
            return {"error": {"message": "Daily query quota exceeded"}}

        @staticmethod
        def raise_for_status():
            raise requests.HTTPError("raw URL contained private-test-key")

    monkeypatch.setattr(
        metadata.requests, "get", lambda *_args, **_kwargs: FakeResponse()
    )

    with pytest.raises(requests.HTTPError) as exc_info:
        metadata._safe_get(
            "https://example.test/books",
            params={"key": "private-test-key"},
            timeout=5,
        )

    message = str(exc_info.value)
    formatted_traceback = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    assert message == "HTTP 429: Daily query quota exceeded"
    assert "private-test-key" not in message
    assert "private-test-key" not in formatted_traceback
