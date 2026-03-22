# media-catalog-books

Refactor de `book_catalog_v0.3` a arquitectura estandarizada con:

- `FastAPI` (backend de servicios)
- `LangGraph` (orquestacion de pipeline)
- `DuckDB` (persistencia)
- `Streamlit` (frontend operativo multipagina)

## Pipeline

1. Ingesta de imagenes de creditos (desde carpeta)
2. OCR + extraccion ISBN
3. Enriquecimiento de metadatos (Google Books, Open Library, ISBNdb)
4. Consolidacion de ficha catalografica
5. Descarga de portada final
6. Exportacion TSV

Todos los estados intermedios se persisten en tablas de DuckDB.
No se generan ficheros JSON intermedios en disco.

## Estructura de entrada (obligatoria)

`data/input` debe seguir esta estructura:

```text
data/input/
  A/
    01/
    02/
    ...
  B/
    01/
    02/
    ...
  C/
    01/
    02/
    ...
```

- `A`, `B`, `C`: bloques de inventario.
- `01..99`: modulos.
- El backend valida esta estructura en la ingesta.

Ademas, el workflow se ejecuta siempre en scope de modulo (`block + module`).

## Persistencia DuckDB

Las tablas principales actuales son:

- `book_items`: estado operativo por libro y columnas de control de pipeline/workflow
- `book_image_files`: imagenes por libro (una fila por imagen, sin ruta absoluta)
- `book_ocr_data`: resultado OCR e ISBN derivados
- `book_bibliographic_sources`: fichas crudas por proveedor (`google`, `isbndb`, `openlibrary`)

Los estados de workflow y de etapas (OCR/metadata/catalog/cover) se centralizan en `book_items`.

La tabla final `books` todavia no se crea en esta fase.
Queda reservada para el volcado final de catalogacion consolidada (a partir de fichas + OCR de creditos).

## Estructura

- `src/backend`: API, servicios, schemas, workflow
- `src/frontend`: app Streamlit y paginas por fase
- `data`: entrada/salida y DuckDB
- `data/output/exports`: exportaciones finales

## Quick start

```bash
cp .env.example .env
make setup
make dev-back
# en otro terminal
make dev-front
```

Tambien puedes levantar ambos servicios con:

```bash
make dev
```

y detenerlos con:

```bash
make stop
```

El `Makefile` incluye comandos multiplataforma (Ubuntu/Linux y Windows) para arranque y parada de backend/frontend.
Ademas, `make dev` verifica/crea `.venv` e instala dependencias si faltan.

## Variables de entorno clave

- `DB_PATH`: ruta DuckDB
- `FRONTEND_THEME_CSS`: ruta CSS para UI (`theme.css` o `theme_legacy.css` en `src/frontend/assets`, o ruta absoluta)
- `COVERS_DIR`: carpeta de entrada de imagenes
- `COVERS_OUTPUT_DIR`: carpeta de portadas descargadas
- `OCR_OUTPUT_DIR`: carpeta opcional con OCR preexistente (`<book_id>.txt`)
- `OCR_PROVIDER`: `auto`, `openai`, `ollama` o `none` (default: `ollama`)
- `OPENAI_API_KEY`: habilita OCR con OpenAI
- `OCR_OPENAI_MODEL`: modelo OCR para OpenAI
- `OCR_OLLAMA_MODEL`: modelo OCR multimodal para Ollama (default: `glm-ocr:latest`)
- `OCR_RESIZE_TO_1800_DEFAULT`: default del checkbox de UI para reducir imagen a 1800 px antes de OCR con glm-ocr (`true` por defecto)
- `OCR_OLLAMA_MODEL_SUGGESTIONS`: sugerencias CSV para UI de modelo OCR Ollama (si no está instalado en backend se muestra en gris y no se puede seleccionar)
- `OCR_ISBN_OLLAMA_MODEL`: modelo Ollama para extraer ISBN desde el texto OCR (default: `gpt-oss:20b`)
- `OCR_OLLAMA_FALLBACK_MODELS`: lista CSV opcional de modelos OCR de respaldo en Ollama (default: vacio, sin fallback)
- `OCR_USE_SIDECAR`: si `true`, usa `OCR_OUTPUT_DIR/<book_id>.txt`; por defecto `false` para OCR real sobre imagen
- `OLLAMA_BASE_URL`: URL base del servicio Ollama
- `OLLAMA_TIMEOUT_SECONDS`: timeout para llamadas a Ollama (vacio = sin timeout, valor recomendado si quieres limitar: `120`)
- `CATALOG_MODEL`: compatibilidad hacia atras (fallback de modelo catalogo)
- `CATALOG_PROVIDER`: `openai` u `ollama` (si está en `.env`, manda ese valor; fallback interno del backend: `openai`)
- `CATALOG_OPENAI_MODEL`: modelo de arbitraje para OpenAI
- `CATALOG_OLLAMA_MODEL`: modelo de arbitraje para Ollama
- `CATALOG_OLLAMA_MODEL_SUGGESTIONS`: sugerencias CSV para UI de modelo catalogo Ollama (si no está instalado en backend se muestra en gris y no se puede seleccionar)
- `CATALOG_ARBITER_ENABLED`: activa arbitraje LLM en casos dudosos
- `CATALOG_ARBITER_PROVIDER`: `auto`, `openai`, `ollama` o `none`
- `CATALOG_ARBITER_MIN_CONFIDENCE`: umbral para disparar arbitraje
- `ISBNDB_API_KEY`: clave para ISBNdb
- `WORKFLOW_MAX_ATTEMPTS`: reintentos automaticos por item

## Temas visuales

Puedes alternar el tema de Streamlit cambiando solo la ruta en `.env`:

```bash
# tema nuevo
FRONTEND_THEME_CSS=theme.css

# tema inspirado en book_catalog_v0.3
FRONTEND_THEME_CSS=theme_legacy.css
```

## Resolucion catalografica

La fase de catalogacion usa reglas deterministas por campo:

- Normalizacion por fuente (`google`, `open_library`, `isbndb`) a esquema comun
- Resolucion por consenso entre fuentes y desempate por prioridad
- Regla especial para editorial: preferencia de nombre comercial sobre forma fiscal
- Trazabilidad (`provenance`) y calidad (`qa.confidence`, `qa.review_flags`) en `catalog`
- Arbitro LLM opcional para conflictos/ambiguedades con validacion determinista posterior
- Si `qa.requires_manual_review=true`, el workflow marca automaticamente el libro en cola de review

## Endpoints principales

- `POST /covers/ingest`
- `GET /models/ollama`
- `POST /workflow/run`
- `GET /workflow/graph`
- `GET /workflow/snapshot`
- `POST /workflow/review/{book_id}`
- `GET /books`, `GET /books/{book_id}`
- `GET /export/books/tsv`
