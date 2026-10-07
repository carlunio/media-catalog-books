# Metadatos bibliográficos y portadas

Esta guía describe el contrato operativo de la fase `metadata` y de la rama
opcional de portadas. Ambas trabajan sobre el ámbito de bloque y módulo
seleccionado en la interfaz.

## Consulta y persistencia de metadatos

La fase de metadatos necesita un ISBN válido. Consulta Google Books, Open
Library e ISBNdb y conserva la respuesta de cada proveedor en
`book_bibliographic_sources`. Cada fuente mantiene su propio estado:
`fetched`, `empty` o `error`.

Un fallo aislado no invalida las respuestas de los demás proveedores. El
resultado general puede quedar como parcial y el lote continúa con el libro
siguiente. Las respuestas se guardan como JSON por proveedor; la separación
entre obra y edición no añade columnas a la base de datos.

Para repetir consultas ya guardadas hay que ejecutar desde `metadata` con
**Sobrescribir etapas ya completas**. Los elementos terminados, en ejecución,
pendientes de revisión o con la ficha consolidada siguen excluidos del flujo
principal.

## Google Books

Google Books requiere `GOOGLE_BOOKS_API_KEY`. La creación del proyecto, la
activación de Books API y las restricciones de la clave se explican en
[`CONFIGURATION.md`](CONFIGURATION.md#configurar-google-books). El cliente envía
la clave en la cabecera `X-Goog-Api-Key` y prueba primero una búsqueda directa
mediante `isbn:`.

Google Books puede devolver falsos resultados vacíos para sus operadores de
campo. Cuando ocurre, la aplicación:

1. obtiene títulos candidatos de Open Library e ISBNdb;
2. realiza una búsqueda ordinaria por título;
3. acepta una ficha únicamente si alguno de sus identificadores coincide con
   el ISBN solicitado o con su equivalente ISBN-10/ISBN-13;
4. registra un error de proveedor si el buscador por campos no funciona y no
   existe una coincidencia segura.

La ficha de Google guarda cómo se obtuvo el resultado:

- `lookup_method: "isbn"`: búsqueda directa por ISBN;
- `lookup_method: "title_validated_by_isbn"`: búsqueda por título validada
  después mediante el ISBN.

Las respuestas antiguas no adquieren este indicador automáticamente. Para
incorporarlo hay que repetir la fase de metadatos con sobrescritura.

## Open Library

Open Library no necesita clave, pero las peticiones se identifican mediante
`OPENLIBRARY_CONTACT`. El formulario de registro y los valores recomendados se
detallan en [`CONFIGURATION.md`](CONFIGURATION.md#configurar-open-library).
Para cada ISBN se realizan dos consultas complementarias:

- `/isbn/{isbn}.json` aporta los datos de la edición concreta;
- Search API aporta un contexto reducido de la obra: título, autoría,
  descripción, primera frase, materias y primer año de publicación.

Los dos niveles se conservan dentro del JSON existente:

```json
{
  "requested_isbn": "9780000000000",
  "edition": {},
  "work": {}
}
```

La catalogación presenta al modelo la edición como
`edition_for_requested_isbn` y la obra como `work_context`. Editorial, fecha,
edición, páginas, encuadernación, idioma y colección deben proceder de la
edición concreta o de la página de créditos. La sinopsis y las materias de la
obra sí se conservan para decidir título, autoría, categoría, género y palabras
clave.

Las respuestas históricas de Search API que agregaban cientos de ediciones se
siguen admitiendo, pero sus listas de editoriales, fechas, ISBN y medianas de
páginas no se usan como datos de una edición concreta.

## ISBNdb

ISBNdb conserva su respuesta original bajo la clave `book`. Antes de construir
el prompt de catalogación, la aplicación separa sus campos en dos grupos:

- edición: ISBN, editorial, fecha, edición, páginas, encuadernación e idioma;
- obra: título, autoría, sinopsis y materias.

No se envían al modelo precios, imágenes, dimensiones en bruto, otros ISBN ni
campos técnicos repetidos. Las dimensiones estructuradas se transforman de
forma determinista a unidades métricas fuera del modelo.

## Descarga automática de portadas

La casilla **Descargar portadas al obtener metadata** está marcada por defecto.
Cuando el rango ejecutado incluye `metadata`, la aplicación recorre todas las
fichas con metadatos del módulo, aunque no haya ningún elemento pendiente de
esa fase.

La decisión se basa en los archivos físicos, no solo en `cover_status`. Se
considera que un ID ya tiene portada cuando existe uno de estos archivos:

```text
data/output/covers/<BLOQUE>/<MÓDULO>/<ID>.jpg
data/output/covers/<BLOQUE>/<MÓDULO>/<ID>.jpeg
data/output/covers/<BLOQUE>/<MÓDULO>/<ID>.png
data/output/covers/<BLOQUE>/<MÓDULO>/<ID>.webp
```

Si se borra el archivo, el ID vuelve a ser candidato en el siguiente barrido
aunque la base de datos todavía conserve `cover_status=downloaded`. Si se
coloca manualmente una portada con el nombre y la extensión esperados, el
barrido la respeta.

### Prioridad de candidatos

1. Se descargan primero las variantes `large` y `medium` de Open Library y
   Google.
2. Si alguna es válida, se elige dentro de ese grupo el archivo de mayor tamaño.
3. Si ninguna existe o todas fallan, se prueban los demás candidatos de Open
   Library, Google e ISBNdb y se conserva el archivo de mayor tamaño.

Esta prioridad impide que la imagen genérica de ISBNdb para libros sin cubierta
desplace una portada `large` o `medium` válida de otra fuente.

La descarga no bloquea la catalogación. Un error afecta únicamente al estado de
la portada de ese libro y el barrido continúa. La rama manual de portadas
permanece disponible después de consolidar la ficha y permite forzar una nueva
descarga mediante **Volver a descargar portadas ya procesadas**.

## Límites y tiempos de espera

Los lotes no tienen un tiempo de espera global. Cada petición bibliográfica y
cada descarga usan `REQUEST_TIMEOUT_SECONDS`; cada llamada de OCR o
catalogación usa su propio límite. El contador se reinicia en cada operación y
en cada libro.

`GOOGLE_BOOKS_MIN_INTERVAL_SECONDS` y
`OPENLIBRARY_MIN_INTERVAL_SECONDS` controlan el intervalo mínimo entre
peticiones de cada proveedor. Los reintentos de un elemento no reinician ni
detienen el resto del lote.
