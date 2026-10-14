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

FORM_FIELD_ORDER: tuple[str, ...] = (
    "tipo_articulo",
    "titulo",
    "titulo_corto",
    "subtitulo",
    "titulo_completo",
    "autor",
    "pais_autor",
    "editorial",
    "pais_publicacion",
    "anio",
    "isbn",
    "idioma",
    "edicion",
    "numero_impresion",
    "coleccion",
    "numero_coleccion",
    "obra_completa",
    "volumen",
    "traductor",
    "ilustrador",
    "editor",
    "fotografia_de",
    "introduccion_de",
    "epilogo_de",
    "ilustraciones",
    "estado_stock",
    "estado_carga",
    "categoria",
    "genero",
    "palabras_clave",
    "encuadernacion",
    "detalle_encuadernacion",
    "estado_conservacion",
    "estado_cubierta",
    "desperfectos",
    "dedicatorias",
    "paginas",
    "alto",
    "ancho",
    "fondo",
    "peso",
    "url_imagenes",
    "plantilla_envio",
    "cantidad",
    "precio",
    "catalogo_1",
    "catalogo_2",
    "catalogo_3",
    "descripcion",
)

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

MULTI_VALUE_FIELDS: frozenset[str] = frozenset(
    {
        *PERSON_NAME_FIELDS,
        "pais_autor",
        "pais_publicacion",
        "idioma",
        "palabras_clave",
        "url_imagenes",
    }
)

FORM_FIELD_HELP: dict[str, str] = {
    **{
        field: (
            "Escribe cada persona como «Apellidos, Nombre». Separa varias "
            "personas con punto y coma (;). No reordenes automáticamente "
            "seudónimos, entidades ni nombres dudosos."
        )
        for field in PERSON_NAME_FIELDS
    },
    "pais_autor": (
        "Escribe el país en español. Si hay varios autores de países distintos, "
        "separa los países con punto y coma (;), conserva el orden de Autor y no "
        "repitas valores."
    ),
    "pais_publicacion": (
        "Escribe el país en español. Si hay varios países de publicación, "
        "sepáralos con punto y coma (;)."
    ),
    "anio": "Escribe el año de publicación de esta edición con cuatro cifras.",
    "isbn": (
        "Escribe un ISBN-10 o ISBN-13 sin espacios ni guiones. En un ISBN-10 "
        "se admite X como último carácter."
    ),
    "idioma": (
        "Escribe el nombre del idioma en español y en minúsculas. Separa "
        "varios idiomas con punto y coma (;)."
    ),
    "numero_coleccion": (
        "Escribe solo el número entero que ocupa el libro en la colección, sin "
        "«nº» ni otros textos."
    ),
    "volumen": (
        "Escribe solo el número romano en mayúsculas, sin «vol.» (por ejemplo, "
        "«IV»). Para un intervalo usa «I-III» y para volúmenes separados, "
        "«I, III». También se admite «OBRA COMPLETA»."
    ),
    "palabras_clave": (
        "Escribe etiquetas temáticas en español separadas por comas. También se "
        "aceptan punto y coma o saltos de línea. La marca NOISBN se gestiona "
        "automáticamente según el ISBN."
    ),
    "paginas": "Escribe un número entero sin añadir «pág.» ni otras unidades.",
    "alto": "Escribe el alto en centímetros como número entero, sin la unidad.",
    "ancho": "Escribe el ancho en centímetros como número entero, sin la unidad.",
    "fondo": "Escribe el fondo en centímetros como número entero, sin la unidad.",
    "peso": "Escribe el peso en gramos como número entero, sin la unidad.",
    "url_imagenes": (
        "Escribe nombres de imagen normalizados y sepáralos con espacios; por "
        "ejemplo: «03B0001.jpg 03B0001_2.jpg»."
    ),
    "cantidad": "Escribe el número entero de ejemplares disponibles.",
    "precio": (
        "Escribe el importe sin el símbolo del euro. Se admiten coma o punto "
        "como separador decimal y se guardan dos decimales."
    ),
}


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
