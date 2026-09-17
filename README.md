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
3. `02_revision_manual`: corrección manual de OCR/ISBN, aceptación explícita de
   libros sin ISBN y salida de review.
4. `03_formulario`: creación manual, edición y consolidación de la ficha final
   (`books`), incluso cuando no existe catalogación automática.
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
La política para evolucionar su esquema está documentada en
`docs/MIGRATIONS.md`.

El ciclo operativo `sin ficha -> borrador -> consolidada`, sus bloqueos y la
reapertura explícita están documentados en
[`docs/FORM_LIFECYCLE.md`](docs/FORM_LIFECYCLE.md).

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

- `book_items`: estado operativo por item, control de workflow y ciclo de la
  ficha (`form_status`).
- `book_image_files`: una fila por imagen asociada a item.
- `book_ocr_data`: texto OCR e ISBN derivados/consolidados.
- `book_bibliographic_sources`: payload por proveedor (`google`, `openlibrary`, `isbndb`).
- `books`: tabla core editable en formulario final.
- `book_field_allowed_values`: valores cerrados para campos del formulario.
- `ref.iso_639_3`: referencia de idiomas ISO 639-3 con `spa_name`.
- `libros_carga_abebooks`: vista de exportación.
- `schema_migrations`: registro de migraciones aplicadas.

## Preparación de recursos locales

Los recursos de `assets/` no se versionan. Se preparan con:

```bash
make prepare-assets
```

Este target se ejecuta automáticamente desde `make setup` y comprueba lo
siguiente:

- Descarga y valida `assets/iso-639-3.tab` desde la
  [tabla oficial de SIL International](https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3.tab).
  Este fichero es necesario para poblar `ref.iso_639_3`.
- Conserva `assets/dani.png` como recurso local opcional. Se puede copiar
  manualmente o definir `APP_ICON_URL` en `.env` para descargar un PNG. La
  aplicación funciona sin él usando el icono por defecto de Streamlit.

Para volver a descargar los recursos configurados:

```bash
python3 scripts/prepare_local_assets.py --force
```

## Inicio rápido

`GNU Make` es opcional y queda reservado como comodidad para desarrollo. Para
los lanzadores sólo se necesitan Python 3.12 o posterior y Git.

```bash
python3 scripts/appctl.py setup
python3 scripts/appctl.py launch
```

`setup` crea `.env` desde `.env.example` cuando falta y nunca sobrescribe una
configuración existente.

También se puede usar `tools/set-up-app.*` una vez y después
`tools/launch-app.*`. El diagnóstico de la instalación está disponible con
`tools/doctor-app.*`. El lanzador normal comprueba `origin/main` en cada
apertura, aplica automáticamente una actualización estable y continúa con la
versión instalada cuando no hay conexión.

Flujo equivalente para desarrollo:

```bash
make setup
make dev
```

Servicios por defecto:

- Backend: `http://127.0.0.1:8000`
- Frontend: `http://127.0.0.1:8501`

Parada:

```bash
python3 scripts/appctl.py stop
```

## Comandos appctl

- `python3 scripts/appctl.py setup`: prepara la instalación.
- `python3 scripts/appctl.py launch`: actualiza desde `main`, ejecuta
  migraciones y arranca en modo estable.
- `python3 scripts/appctl.py dev`: arranca con recarga y sin actualizar Git.
- `python3 scripts/appctl.py update`: actualiza explícitamente una instalación
  limpia situada en `main`.
- `python3 scripts/appctl.py stop`: detiene la instancia gestionada.
- `python3 scripts/appctl.py doctor`: comprueba la instalación.
- `python3 scripts/appctl.py smoke`: arranca ambos servicios con una base
  temporal y comprueba su salud.

## Comandos Make para desarrollo

- `make prepare-assets`: descarga y valida los recursos locales necesarios.
- `make setup`: prepara recursos, crea `.venv` e instala dependencias.
- `make install`: reinstala dependencias del proyecto.
- `make lock`: sincroniza `requirements.lock` conservando las versiones fijadas.
- `make upgrade-lock`: actualiza el lock dentro de los rangos de `pyproject.toml`.
- `make check-lock`: comprueba que declaración y lock coinciden.
- `make build`: genera la rueda y el paquete fuente en `dist/`.
- `make update`: aplica el actualizador estable de `appctl`.
- `make start`: actualización automática y arranque estable.
- `make dev`: arranque de desarrollo sin actualización y con recarga.
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
- `make lint`: ejecuta Ruff y comprueba el formato con Black.
- `make format`: ejecuta Black.
- `make test`: ejecuta Pytest.
- `make stop`: detiene backend y frontend.
- `make doctor`: ejecuta el diagnóstico de `appctl`.
- `make smoke`: prueba backend y frontend sin tocar los datos reales.

El contrato de dependencias, su actualización y la validación multiplataforma
se detallan en [`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md).

## Configuración por `.env`

La aplicación puede abrirse con los valores generados por `setup`. Ollama es el
proveedor local predeterminado; las claves de OpenAI e ISBNdb son opcionales.
La referencia completa, validaciones y resolución de problemas están en
[`docs/CONFIGURATION.md`](docs/CONFIGURATION.md).

Preparación local:

- `APP_ICON_URL`: URL opcional para descargar `assets/dani.png` durante la preparación.

Actualización y arranque:

- `GIT_REMOTE`: remoto del canal estable; por defecto `origin`.
- `GIT_BRANCH`: rama estable; debe ser `main` en instalaciones de usuario.
- `APP_UPDATE_TIMEOUT_SECONDS`: espera máxima de la comprobación remota.
- `APP_STARTUP_TIMEOUT_SECONDS`: espera máxima de salud tras arrancar.
- `APP_UPDATE_BACKUP_DIR`: backups locales previos a actualizar.
- `APP_UPDATE_BACKUP_KEEP`: número de backups recientes conservados.
- `BACK_HOST` / `BACK_PORT`: escucha local del backend.
- `FRONT_HOST` / `FRONT_PORT`: escucha local del frontend.

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

Cada snapshot incluye manifiesto JSON con `snapshot_id`, versión de app, versión
real del esquema, origen (`SYNC_ACTOR`/`SYNC_DEVICE`), tamaño y `sha256`.

La importación:

- requiere confirmación explícita (`confirm=true`);
- verifica hash;
- rechaza esquemas desconocidos y bases que no pertenecen a Books;
- valida y migra una copia temporal antes de tocar la base activa;
- crea backup local antes de reemplazar la DB;
- restaura la base anterior si falla el registro final;
- actualiza `SYNC_STATE_PATH`.

La actualización automática de la aplicación no importa snapshots. Sustituir
la base local sigue requiriendo confirmación expresa desde la página de datos.
La operación y sus garantías están detalladas en `docs/SNAPSHOTS.md`.

## Migraciones

El arranque aplica automáticamente las migraciones incrementales pendientes
antes de iniciar los servicios. Cada migración se ejecuta en una transacción y
su checksum incluye la implementación completa, de modo que una versión ya
publicada no se puede modificar silenciosamente.

La compatibilidad se prueba también contra una base sintética generada con la
release `v0.1.1`, verificando que conserva datos, tablas, columnas y el contrato
de exportación. Ver `docs/MIGRATIONS.md` para el procedimiento de desarrollo.

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
- Las migraciones incrementales actualizan el esquema sin alterar los campos de negocio.

## Historial

Para cambios por versión, ver `CHANGELOG.md`.

La política `develop -> main -> tag` y la lista de publicación están
documentadas en `docs/RELEASING.md`.
