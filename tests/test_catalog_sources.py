from src.backend.services import catalog


def test_catalog_prompt_keeps_thematic_context_and_separates_edition_fields():
    metadata = {
        "google": {
            "title": "Título",
            "description": "Sinopsis de Google",
            "categories": ["Historia"],
            "imageLinks": {"thumbnail": "https://example.test/cover.jpg"},
            "averageRating": 4.5,
            "contentVersion": "1.2.3.0.preview.3",
        },
        "open_library": {
            "requested_isbn": "9788412280043",
            "edition": {
                "title": "Título",
                "publishers": ["Editorial exacta"],
                "publish_date": "2021",
                "number_of_pages": 160,
                "physical_format": "Paperback",
                "isbn_13": ["9788412280043"],
                "cover": {"large": "https://example.test/cover.jpg"},
                "revision": 19,
                "works": [{"key": "/works/OL1W"}],
            },
            "work": {
                "title": "Título",
                "authors": [{"name": "Autora, Ana"}],
                "description": "Sinopsis útil",
                "subject": ["Historia", "Memoria"],
                "first_publish_year": 1998,
                "key": "/works/OL1W",
                "url": "https://openlibrary.org/works/OL1W",
            },
        },
        "isbndb": {
            "book": {
                "isbn13": "9788412280043",
                "publisher": "Editorial exacta",
                "date_published": "2021",
                "pages": 160,
                "binding": "Paperback",
                "title": "Título",
                "authors": ["Ana Autora"],
                "synopsis": "Otra sinopsis útil",
                "subjects": ["Historia"],
                "image": "https://example.test/isbn-cover.jpg",
                "dimensions": "8 x 5 x 1 inches",
                "dimensions_structured": {"height": {"value": 8, "unit": "inches"}},
                "msrp": 20.0,
                "list_price": {"amount": 18.0},
                "other_isbns": [{"isbn": "0000000000"}],
            }
        },
    }

    google, open_library, isbndb = catalog._clean_sources_for_prompt(metadata)

    assert google["description"] == "Sinopsis de Google"
    assert google["categories"] == ["Historia"]
    assert "imageLinks" not in google
    assert "averageRating" not in google
    assert "contentVersion" not in google

    assert open_library == {
        "requested_isbn": "9788412280043",
        "edition_for_requested_isbn": {
            "title": "Título",
            "publishers": ["Editorial exacta"],
            "publish_date": "2021",
            "number_of_pages": 160,
            "physical_format": "Paperback",
            "isbn_13": ["9788412280043"],
        },
        "work_context": {
            "title": "Título",
            "authors": [{"name": "Autora, Ana"}],
            "description": "Sinopsis útil",
            "subject": ["Historia", "Memoria"],
            "first_publish_year": 1998,
        },
    }
    assert isbndb == {
        "edition_for_requested_isbn": {
            "isbn13": "9788412280043",
            "publisher": "Editorial exacta",
            "date_published": "2021",
            "pages": 160,
            "binding": "Paperback",
        },
        "work_context": {
            "title": "Título",
            "authors": ["Ana Autora"],
            "synopsis": "Otra sinopsis útil",
            "subjects": ["Historia"],
        },
    }


def test_legacy_open_library_aggregate_is_only_work_context():
    _google, open_library, _isbndb = catalog._clean_sources_for_prompt(
        {
            "open_library": {
                "title": "Obra",
                "authors": [{"name": "Autora"}],
                "subject": ["Poesía"],
                "publishers": [f"Editorial {index}" for index in range(300)],
                "publish_date": "1850",
                "number_of_pages": 999,
                "identifiers": {
                    "isbn_13": [str(index).zfill(13) for index in range(500)]
                },
            }
        }
    )

    assert open_library == {
        "work_context": {
            "title": "Obra",
            "authors": [{"name": "Autora"}],
            "subject": ["Poesía"],
        }
    }


def test_catalog_prompt_explains_work_context_usage():
    assert "sinopsis, descripción y temas/materias" in catalog.CATALOG_SYSTEM_PROMPT
    assert "edition_for_requested_isbn" in catalog.CATALOG_SYSTEM_PROMPT
