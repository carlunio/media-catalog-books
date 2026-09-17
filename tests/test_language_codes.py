import iso639

from src.backend.language_codes import idioma_es_a_iso639_3


def test_declared_language_dependencies_expose_the_expected_api():
    assert hasattr(iso639, "Language")
    assert idioma_es_a_iso639_3("español") == "SPA"
