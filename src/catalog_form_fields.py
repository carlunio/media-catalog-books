from __future__ import annotations

FORM_FIELD_LABELS: dict[str, str] = {
    "tipo_articulo": "Tipo de artículo",
    "estado_stock": "Estado de stock",
    "estado_carga": "Estado de carga",
    "titulo": "Título",
    "titulo_corto": "Título corto",
    "subtitulo": "Subtítulo",
    "titulo_completo": "Título completo",
    "autor": "Autor",
    "pais_autor": "País del autor",
    "editorial": "Editorial",
    "isbn": "ISBN",
    "pais_publicacion": "País de la publicación",
    "idioma": "Idioma",
    "anio": "Año de publicación",
    "edicion": "Edición",
    "numero_impresion": "Número de impresión",
    "coleccion": "Colección",
    "numero_coleccion": "Nº en la colección",
    "obra_completa": "Título de la obra completa",
    "volumen": "Volumen",
    "traductor": "Traductor",
    "ilustrador": "Ilustrador",
    "editor": "Editor",
    "fotografia_de": "Fotografía de",
    "introduccion_de": "Introducción de",
    "epilogo_de": "Epílogo de",
    "ilustraciones": "Info. sobre ilustraciones",
    "categoria": "Categoría",
    "genero": "Género",
    "palabras_clave": "Palabras clave",
    "encuadernacion": "Encuadernación",
    "detalle_encuadernacion": "Detalles de la encuadernación",
    "estado_conservacion": "Estado de conservación",
    "estado_cubierta": "Estado de la cubierta",
    "desperfectos": "Desperfectos",
    "dedicatorias": "Dedicatorias",
    "paginas": "Nº de páginas",
    "alto": "Alto",
    "peso": "Peso",
    "ancho": "Ancho",
    "fondo": "Fondo",
    "url_imagenes": "URL de imágenes",
    "plantilla_envio": "Plantilla de envío",
    "cantidad": "Cantidad",
    "precio": "Precio",
    "catalogo_1": "Catálogo 1",
    "catalogo_2": "Catálogo 2",
    "catalogo_3": "Catálogo 3",
    "descripcion": "Descripción",
}

FORM_FIELD_ORDER: tuple[str, ...] = tuple(FORM_FIELD_LABELS)

READ_ONLY_FIELDS: frozenset[str] = frozenset(
    {"titulo_corto", "subtitulo", "titulo_completo"}
)

SELECTABLE_FIELDS: frozenset[str] = frozenset(
    {
        "tipo_articulo",
        "estado_stock",
        "estado_carga",
        "edicion",
        "numero_impresion",
        "ilustraciones",
        "categoria",
        "genero",
        "encuadernacion",
        "estado_conservacion",
        "estado_cubierta",
        "dedicatorias",
        "plantilla_envio",
        "catalogo_1",
        "catalogo_2",
        "catalogo_3",
    }
)

SELECTABLE_WITH_CUSTOM_VALUE_FIELDS: frozenset[str] = frozenset({"categoria", "genero"})

SALMON_FIELDS: frozenset[str] = frozenset(
    {
        "edicion",
        "numero_impresion",
        "coleccion",
        "numero_coleccion",
        "obra_completa",
        "volumen",
    }
)

IMPORTANT_FIELDS: frozenset[str] = frozenset(
    {
        "edicion",
        "coleccion",
        "numero_coleccion",
        "obra_completa",
        "volumen",
        "detalle_encuadernacion",
        "desperfectos",
        "url_imagenes",
        "plantilla_envio",
        "precio",
    }
)

PERSON_NAME_FIELDS: frozenset[str] = frozenset(
    {
        "autor",
        "traductor",
        "ilustrador",
        "editor",
        "fotografia_de",
        "introduccion_de",
        "epilogo_de",
    }
)

LABEL_COLORS: dict[str, str] = {
    "lbl-blue": "A4B8D3",
    "lbl-lilac": "BDB1CF",
    "lbl-orange": "E2BF9F",
    "lbl-salmon": "DBB0A3",
    "lbl-beige": "E2D2C4",
    "lbl-green": "BCCCAB",
    "lbl-yellow": "E8DDA0",
    "lbl-cyan": "A6CED8",
    "lbl-purple": "B9ADC9",
    "lbl-steel": "9CAFC7",
    "lbl-orange-soft": "ECDBC8",
}


def label_class(field: str) -> str:
    if field == "descripcion":
        return "lbl-green"
    if field in READ_ONLY_FIELDS:
        return "lbl-orange-soft"
    if field in SALMON_FIELDS:
        return "lbl-salmon"
    if field in {"tipo_articulo", "categoria", "genero", "palabras_clave"}:
        return "lbl-blue"
    if field in {
        "estado_stock",
        "estado_carga",
        "plantilla_envio",
        "catalogo_1",
        "catalogo_2",
        "catalogo_3",
        "cantidad",
        "precio",
        "url_imagenes",
    }:
        return "lbl-purple"
    if field in {"titulo", "subtitulo"}:
        return "lbl-orange"
    if field in {
        "titulo_corto",
        "titulo_completo",
        "obra_completa",
        "volumen",
        "coleccion",
        "numero_coleccion",
    }:
        return "lbl-beige"
    if field in {
        "autor",
        "pais_autor",
        "editorial",
        "pais_publicacion",
        "anio",
        "isbn",
        "idioma",
    }:
        return "lbl-green"
    if field in {
        "encuadernacion",
        "detalle_encuadernacion",
        "estado_conservacion",
        "estado_cubierta",
        "desperfectos",
        "dedicatorias",
    }:
        return "lbl-yellow"
    if field in {"paginas", "peso", "alto", "ancho", "fondo"}:
        return "lbl-cyan"
    return "lbl-steel"


def label_color(field: str) -> str:
    return LABEL_COLORS[label_class(field)]


def allowed_values_key(field: str) -> str:
    return "catalogo" if field.startswith("catalogo_") else field
