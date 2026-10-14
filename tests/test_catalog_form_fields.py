from src.catalog_form_fields import (
    FORM_FIELD_HELP,
    FORM_FIELD_LABELS,
    FORM_FIELD_ORDER,
    MULTI_VALUE_FIELDS,
    PERSON_NAME_FIELDS,
)


def test_form_field_order_is_complete_unique_and_groups_dimensions():
    assert len(FORM_FIELD_ORDER) == len(set(FORM_FIELD_ORDER))
    assert set(FORM_FIELD_ORDER) == set(FORM_FIELD_LABELS)

    dimension_positions = [
        FORM_FIELD_ORDER.index(field) for field in ("alto", "ancho", "fondo", "peso")
    ]
    assert dimension_positions == list(
        range(dimension_positions[0], dimension_positions[0] + 4)
    )


def test_special_format_fields_have_help():
    assert PERSON_NAME_FIELDS <= FORM_FIELD_HELP.keys()
    assert MULTI_VALUE_FIELDS <= FORM_FIELD_HELP.keys()
    assert {
        "anio",
        "isbn",
        "pais_publicacion",
        "numero_coleccion",
        "volumen",
        "paginas",
        "alto",
        "ancho",
        "fondo",
        "peso",
        "cantidad",
        "precio",
        "url_imagenes",
    } <= FORM_FIELD_HELP.keys()
