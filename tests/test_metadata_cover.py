from src.backend.services import covers
from src.backend.workflow import graph


def test_cover_candidates_read_nested_open_library_edition():
    urls = covers._cover_candidates(
        {
            "open_library": {
                "edition": {
                    "cover": {
                        "large": "https://example.test/large.jpg",
                        "small": "https://example.test/small.jpg",
                    }
                }
            }
        }
    )

    assert urls == [
        "https://example.test/large.jpg",
        "https://example.test/small.jpg",
    ]


def test_preferred_cover_candidates_only_include_other_providers_large_medium():
    urls = covers._preferred_cover_candidates(
        {
            "open_library": {
                "edition": {
                    "cover": {
                        "large": "https://openlibrary.test/large.jpg",
                        "small": "https://openlibrary.test/small.jpg",
                    }
                }
            },
            "google": {
                "imageLinks": {
                    "thumbnail": "https://google.test/thumbnail.jpg",
                    "medium": "https://google.test/medium.jpg",
                }
            },
            "isbndb": {"book": {"image": "https://isbndb.test/not-available.jpg"}},
        }
    )

    assert urls == [
        "https://openlibrary.test/large.jpg",
        "https://google.test/medium.jpg",
    ]


def test_large_medium_candidates_win_before_larger_isbndb_file(monkeypatch, tmp_path):
    book_id = "01A0001"
    calls = []
    updates = []
    sizes = {
        "https://openlibrary.test/large.jpg": 20,
        "https://google.test/medium.jpg": 30,
        "https://google.test/thumbnail.jpg": 500,
        "https://isbndb.test/not-available.jpg": 1000,
    }
    book = {
        "id": book_id,
        "cover_status": "pending",
        "cover_path": None,
        "metadata": {
            "open_library": {
                "edition": {"cover": {"large": "https://openlibrary.test/large.jpg"}}
            },
            "google": {
                "imageLinks": {
                    "thumbnail": "https://google.test/thumbnail.jpg",
                    "medium": "https://google.test/medium.jpg",
                }
            },
            "isbndb": {"book": {"image": "https://isbndb.test/not-available.jpg"}},
        },
    }

    def fake_download(url, destination_base, *, timeout):
        calls.append(url)
        path = destination_base.with_suffix(".jpg")
        path.write_bytes(b"x" * sizes[url])
        return path

    monkeypatch.setattr(covers, "_output_dir_for_book", lambda _book_id: tmp_path)
    monkeypatch.setattr(covers.books, "get_book", lambda _book_id: book)
    monkeypatch.setattr(covers, "_download_one", fake_download)
    monkeypatch.setattr(
        covers.books,
        "update_cover",
        lambda target_id, **kwargs: updates.append((target_id, kwargs)),
    )

    result = covers.run_one(book_id)

    assert calls == [
        "https://openlibrary.test/large.jpg",
        "https://google.test/medium.jpg",
    ]
    assert result["status"] == "downloaded"
    assert (tmp_path / f"{book_id}.jpg").stat().st_size == 30
    assert updates[-1][1]["status"] == "downloaded"


def test_without_large_medium_cover_uses_largest_file_among_all(monkeypatch, tmp_path):
    book_id = "01A0001"
    calls = []
    sizes = {
        "https://openlibrary.test/small.jpg": 20,
        "https://google.test/thumbnail.jpg": 30,
        "https://isbndb.test/cover.jpg": 100,
    }
    book = {
        "id": book_id,
        "cover_status": "pending",
        "cover_path": None,
        "metadata": {
            "open_library": {
                "edition": {"cover": {"small": "https://openlibrary.test/small.jpg"}}
            },
            "google": {
                "imageLinks": {
                    "thumbnail": "https://google.test/thumbnail.jpg",
                }
            },
            "isbndb": {"book": {"image": "https://isbndb.test/cover.jpg"}},
        },
    }

    def fake_download(url, destination_base, *, timeout):
        calls.append(url)
        result = destination_base.with_suffix(".jpg")
        result.write_bytes(b"x" * sizes[url])
        return result

    monkeypatch.setattr(covers, "_output_dir_for_book", lambda _book_id: tmp_path)
    monkeypatch.setattr(covers.books, "get_book", lambda _book_id: book)
    monkeypatch.setattr(covers, "_download_one", fake_download)
    monkeypatch.setattr(covers.books, "update_cover", lambda *args, **kwargs: None)

    result = covers.run_one(book_id)

    assert calls == [
        "https://openlibrary.test/small.jpg",
        "https://google.test/thumbnail.jpg",
        "https://isbndb.test/cover.jpg",
    ]
    assert result["status"] == "downloaded"
    assert (tmp_path / f"{book_id}.jpg").stat().st_size == 100


def test_existing_cover_file_is_reused_even_when_database_status_is_stale(
    monkeypatch, tmp_path
):
    book_id = "01A0001"
    existing = tmp_path / f"{book_id}.jpg"
    existing.write_bytes(b"image")
    updates = []

    monkeypatch.setattr(covers, "_output_dir_for_book", lambda _book_id: tmp_path)
    monkeypatch.setattr(
        covers.books,
        "get_book",
        lambda _book_id: {
            "id": book_id,
            "cover_status": "missing",
            "cover_path": None,
            "metadata": {},
        },
    )
    monkeypatch.setattr(
        covers.books,
        "update_cover",
        lambda target_id, **kwargs: updates.append((target_id, kwargs)),
    )

    result = covers.run_one(book_id)

    assert result["status"] == "skipped"
    assert result["cover_path"] == str(existing.resolve())
    assert updates == [
        (
            book_id,
            {
                "cover_path": str(existing.resolve()),
                "status": "downloaded",
                "error": None,
            },
        )
    ]


def test_metadata_node_downloads_cover_by_default_without_blocking(monkeypatch):
    book_id = "01A0001"
    cover_calls = []
    updates = []

    monkeypatch.setattr(
        graph.books, "set_workflow_running", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        graph.metadata,
        "run_one",
        lambda *args, **kwargs: {"id": book_id, "status": "fetched"},
    )
    monkeypatch.setattr(
        graph.books,
        "get_book",
        lambda _book_id: {
            "id": book_id,
            "metadata_status": "fetched",
            "cover_status": "error",
            "cover_path": None,
        },
    )

    def failing_cover(target_id, **kwargs):
        cover_calls.append((target_id, kwargs))
        raise TimeoutError("cover timed out")

    monkeypatch.setattr(graph.covers, "run_one", failing_cover)
    monkeypatch.setattr(
        graph.books,
        "update_cover",
        lambda target_id, **kwargs: updates.append((target_id, kwargs)),
    )

    result = graph._metadata_node(
        {
            "book_id": book_id,
            "start_stage": "metadata",
            "stop_after": "metadata",
            "overwrite": False,
        }
    )

    assert cover_calls == [(book_id, {"overwrite": False})]
    assert updates == [
        (
            book_id,
            {
                "cover_path": None,
                "status": "error",
                "error": "cover timed out",
            },
        )
    ]
    assert result["outcome"] == "stopped_after_metadata"
    assert result["stop_pipeline"] is True


def test_metadata_node_can_disable_automatic_cover(monkeypatch):
    book_id = "01A0001"
    cover_calls = []

    monkeypatch.setattr(
        graph.books, "set_workflow_running", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(
        graph.metadata,
        "run_one",
        lambda *args, **kwargs: {"id": book_id, "status": "fetched"},
    )
    monkeypatch.setattr(
        graph.books,
        "get_book",
        lambda _book_id: {"id": book_id, "metadata_status": "fetched"},
    )
    monkeypatch.setattr(
        graph.covers,
        "run_one",
        lambda *args, **kwargs: cover_calls.append((args, kwargs)),
    )

    graph._metadata_node(
        {
            "book_id": book_id,
            "start_stage": "metadata",
            "stop_after": "metadata",
            "overwrite": False,
            "download_cover_after_metadata": False,
        }
    )

    assert cover_calls == []
