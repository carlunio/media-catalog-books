# media-catalog-books

Aplicación para catalogación de libros con flujo por módulos (bloque + módulo),
backend API, orquestación de etapas, revisión manual, formulario final y
exportación tabulada para carga externa.

## Estado del proyecto

Este README documenta el estado actual del proyecto.

- Historial de cambios y reconstrucción: `CHANGELOG.md`.
- Seguimiento técnico: `ROADMAP.md`.
- Stack principal: FastAPI + LangGraph + DuckDB + Streamlit.
- Arquitectura alineada con `media-catalog-movies` y `media-catalog-vinyls` en versionado, migraciones, snapshots, CI, tests y lanzadores.

## Flujo funcional actual (frontend)

Orden de páginas en la app:

1. `00_extraccion`: alta de imágenes en base de datos para un módulo.
2. `01_orquestacion`: ejecución por lotes/rango de etapas y control operativo.
3. `02_revision_manual`: corrección manual de OCR/ISBN y salida de review.
4. `03_formulario`: edición final de ficha (`books`).
5. `04_exportacion`: salida TXT tabulada para carga externa.
6. `05_datos`: publicación, listado, importación y limpieza de snapshots DuckDB.

Etapas del workflow backend: `ocr -> metadata -> catalog -> cover`.

## Arquitectura

- `src/project_meta.py`: metadatos de proyecto/versionado desde `pyproject.toml`.
- `src/backend/main.py`: composición de la aplicación FastAPI y registro de routers.
- `src/backend/routers`: endpoints separados por dominio (`core`, `ingest`, `workflow`, `books`, `core_books`, `export`, `snapshots`).
- `src/backend/services`: lógica de OCR, metadata, catálogo, covers, exportación, migraciones y snapshots.
- `src/backend/schemas`: contratos Pydantic de payloads API.
- `src/frontend`: app Streamlit multipágina y utilidades compartidas.
- `scripts`: inicialización, migraciones, mantenimiento DB y snapshots.
- `tests`: pruebas de import, esquema, migraciones, exportación y snapshots.

DuckDB sigue siendo la fuente única de verdad del estado operativo y de la ficha final.

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

- Bloques válidos: `A`, `B`, `C`.
- Módulos válidos: `01..99`.
- La ejecución trabaja en scope `block + module`.

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

- `book_items`: estado operativo por item y control de workflow.
- `book_image_files`: una fila por imagen asociada a item.
- `book_ocr_data`: texto OCR e ISBN derivados/consolidados.
- `book_bibliographic_sources`: payload por proveedor (`google`, `openlibrary`, `isbndb`).
- `books`: tabla core editable en formulario final.
- `book_field_allowed_values`: valores cerrados para campos del formulario.
- `ref.iso_639_3`: referencia de idiomas ISO 639-3 con `spa_name`.
- `libros_carga_abebooks`: vista de exportación.
- `schema_migrations`: registro de migraciones aplicadas.

## Inicio rápido

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

- `make setup`: crea `.venv` e instala dependencias.
- `make install`: reinstala dependencias del proyecto.
- `make update-repo`: ejecuta `git pull origin main`.
- `make update`: actualiza repo y reinstala dependencias.
- `make dev`: inicializa DB y levanta backend + frontend.
- `make dev-back`: solo backend.
- `make dev-front`: solo frontend.
- `make init-db`: crea/ajusta esquema de DuckDB mediante migraciones.
- `make migrate-db`: aplica migraciones explícitamente.
- `make db-maint`: mantenimiento ligero de DB.
- `make db-repack`: repack a archivo nuevo.
- `make db-repack-replace`: repack y reemplazo del archivo original.
- `make publish-snapshot`: publica snapshot DuckDB.
- `make list-snapshots`: lista snapshots disponibles.
- `make import-snapshot SNAPSHOT_ID=<id>`: importa un snapshot confirmado.
- `make cleanup-snapshots`: elimina snapshots antiguos según retención.
- `make lint`: ejecuta Ruff.
- `make format`: ejecuta Black.
- `make test`: ejecuta Pytest.
- `make stop`: detiene backend y frontend.

## Configuración por `.env`

Rutas:

- `PROJECT_ROOT`
- `DB_PATH`
- `COVERS_DIR`
- `COVERS_OUTPUT_DIR`
- `EXPORTS_DIR`
- `OCR_OUTPUT_DIR`

Snapshots y sincronización:

- `BBDD_DIR`
- `SYNC_STATE_PATH`
- `SYNC_ACTOR`
- `SYNC_DEVICE`
- `SYNC_RETENTION_DAYS`
- `SYNC_KEEP_MIN`

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

Catalogación automática:

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
- `APP_CHANNEL`
- `FRONTEND_THEME_CSS`

## Exportación

La exportación usa la vista `libros_carga_abebooks` y aplica filtros por bloque/módulo.

- Formato: TXT delimitado por TAB, con cabecera.
- Encoding configurable: `windows-1252` (default) o `utf-8`.
- Endpoint de exportación: `GET /export/books/txt`.
- Endpoint de exportación por selección: `POST /export/books/txt`.
- Validación no bloqueante: `GET/POST /export/books/validate`.
- Descarga de archivo generado: `GET /export/books/file?filename=...`.

Los campos de la ficha final y la vista exportada se mantienen como contrato funcional del proyecto.

## Snapshots

Los snapshots publican una copia compactada de la base DuckDB en:

```text
<BBDD_DIR>/media-catalog-books/snapshots/
```

Cada snapshot incluye manifiesto JSON con `snapshot_id`, versión de app, origen (`SYNC_ACTOR`/`SYNC_DEVICE`), tamaño y `sha256`.

La importación:

- requiere confirmación explícita (`confirm=true`);
- verifica hash;
- crea backup local antes de reemplazar la DB;
- actualiza `SYNC_STATE_PATH`.

## Lanzadores

La carpeta `tools/` contiene lanzadores de doble clic para Windows y Ubuntu/Linux:

- preparar app;
- arrancar app;
- detener app;
- actualizar app.

Ver `tools/README.md`.

## Notas operativas

- No se usan JSON intermedios en disco como mecanismo principal del pipeline.
- El estado operativo vive en DuckDB.
- La revisión manual y el formulario escriben directamente en base de datos.
- Las migraciones registran el baseline del esquema actual sin alterar los campos de negocio.

## Historial

Para cambios por versión, ver `CHANGELOG.md`.
