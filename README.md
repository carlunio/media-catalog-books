# media-catalog-books

Aplicación para catalogación de libros con flujo por módulos (bloque + módulo),
con backend API, orquestación de etapas, revisión manual y exportación final.

## Estado del proyecto

Este README documenta solo el estado actual del proyecto.

- Historial de cambios y reconstruccion: `CHANGELOG.md`.
- Stack principal: FastAPI + LangGraph + DuckDB + Streamlit.

## Flujo funcional actual (frontend)

Orden de páginas en la app:

1. `00_extraccion`: alta de imágenes en base de datos para un módulo
2. `01_orquestacion`: ejecución por lotes/rango de etapas y control operativo
3. `02_revision_manual`: corrección manual de OCR/ISBN y salida de review
4. `03_formulario`: edicion final de ficha (`books`)
5. `04_exportacion`: salida TXT tabulado para carga externa

Etapas del workflow backend: `ocr -> metadata -> catalog -> cover`.

## Arquitectura

- `src/backend`
- API FastAPI (`src.backend.main:app`)
- servicios de OCR, metadata, catalogo, covers, export
- orquestación LangGraph y estado de workflow
- `src/frontend`
- app Streamlit multipágina
- UI de orquestación, revisión y formulario final
- `DuckDB`
- persistencia única de estados intermedios y resultado final

## Estructura de datos de entrada/salida

Estructura requerida en `data/input`:

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

- Bloques validos: `A`, `B`, `C`.
- Modulos validos: `01..99`.
- La ejecución siempre trabaja en scope `block + module`.

Salida de portadas descargadas:

```text
data/output/covers/<BLOQUE>/<MODULO>/
```

Salida de exportaciones:

```text
data/output/exports/
```

## Modelo de datos (DuckDB)

Tablas/vistas principales:

- `book_items`: estado operativo por item y control de workflow
- `book_image_files`: una fila por imagen asociada a item
- `book_ocr_data`: texto OCR e ISBN derivados/consolidados
- `book_bibliographic_sources`: payload por proveedor (`google`, `openlibrary`, `isbndb`)
- `books`: tabla core editable en formulario final
- `book_field_allowed_values`: valores cerrados para campos del formulario
- `ref.iso_639_3`: referencia de idiomas ISO 639-3 con `spa_name`
- `libros_carga_abebooks` (view): vista de exportación

## Inicio rapido

```bash
cp .env.example .env
make setup
make dev
```

Servicios por defecto:

- Backend: `http://127.0.0.1:8000`
- Frontend: `http://127.0.0.1:8501`

Parada:

```bash
make stop
```

## Comandos Make relevantes

- `make setup`: crea `.venv` e instala dependencias
- `make dev`: inicializa DB y levanta backend + frontend
- `make dev-back`: solo backend
- `make dev-front`: solo frontend
- `make init-db`: crea/ajusta esquema de DuckDB
- `make db-maint`: mantenimiento ligero de DB
- `make db-repack`: repack a archivo nuevo
- `make db-repack-replace`: repack y reemplazo del archivo original
- `make stop`: detiene backend y frontend

## Configuración por .env (claves principales)

Rutas:

- `DB_PATH`
- `COVERS_DIR`
- `COVERS_OUTPUT_DIR`
- `OCR_OUTPUT_DIR`

OCR:

- `OCR_PROVIDER` (`ollama` u `openai`)
- `OCR_OLLAMA_MODEL`
- `OCR_OPENAI_MODEL`
- `OCR_RESIZE_TO_1800_DEFAULT`
- `OCR_ISBN_OLLAMA_MODEL`
- `OCR_OLLAMA_FALLBACK_MODELS`
- `OCR_USE_SIDECAR`
- `OLLAMA_BASE_URL`
- `OLLAMA_TIMEOUT_SECONDS`

Catalogación automatica:

- `CATALOG_PROVIDER` (`ollama` u `openai`)
- `CATALOG_OLLAMA_MODEL`
- `CATALOG_OPENAI_MODEL`
- `CATALOG_OLLAMA_MODEL_SUGGESTIONS`
- `CATALOG_ARBITER_ENABLED`
- `CATALOG_ARBITER_PROVIDER`
- `CATALOG_ARBITER_MIN_CONFIDENCE`

APIs y límites:

- `OPENAI_API_KEY`
- `ISBNDB_API_KEY`
- `REQUEST_TIMEOUT_SECONDS`
- `WORKFLOW_MAX_ATTEMPTS`
- `GOOGLE_BOOKS_MIN_INTERVAL_SECONDS`
- `OPENLIBRARY_MIN_INTERVAL_SECONDS`

Frontend:

- `API_URL`
- `API_TIMEOUT_SECONDS`
- `API_LONG_TIMEOUT_SECONDS`
- `FRONTEND_THEME_CSS`

## Exportación

La exportación usa la vista `libros_carga_abebooks` y aplica filtros por bloque/módulo.

- Formato: TXT delimitado por TAB, con cabecera
- Encoding configurable: `windows-1252` (default) o `utf-8`
- Endpoint: `GET /export/books/txt`
- Descarga de archivo generado: `GET /export/books/file?filename=...`

## Notas operativas

- No se usan JSON intermedios en disco como mecanismo principal del pipeline.
- El estado operativo vive en DuckDB.
- La revisión manual y el formulario escriben directamente en base de datos.

## Historial

Para cambios por version, ver `CHANGELOG.md`.
