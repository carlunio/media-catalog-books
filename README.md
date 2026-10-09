# media-catalog-books

Aplicación para catalogar libros mediante un flujo por módulos (bloque +
módulo), una API de backend, orquestación de etapas, revisión manual, formulario
final y exportación tabulada para carga externa.

## Estado del proyecto

Este README documenta el estado actual del proyecto.

- Historial de cambios y reconstrucción: `CHANGELOG.md`.
- Seguimiento técnico: `ROADMAP.md`.
- Pila tecnológica principal: FastAPI + LangGraph + DuckDB + Streamlit.
- Arquitectura alineada con `media-catalog-movies` y `media-catalog-vinyls`
  en versionado, migraciones, instantáneas, integración continua, pruebas y
  lanzadores.

## Flujo funcional actual (interfaz)

Orden de páginas en la aplicación:

1. `00_extraccion`: alta de imágenes en base de datos para un módulo.
2. `01_orquestacion`: ejecución por lotes o rangos de etapas y control
   operativo.
3. `02_revision_manual`: corrección manual de OCR/ISBN, aceptación explícita de
   libros sin ISBN y salida de revisión.
4. `03_formulario`: creación manual, edición y consolidación de la ficha final
   (`books`), incluso cuando no existe catalogación automática.
5. `04_exportacion`: salida TXT tabulada para carga externa.
6. `05_datos`: publicación, listado, importación y limpieza de instantáneas de
   DuckDB.

Flujo principal del backend: `ocr -> metadata -> catalog`. Al terminar
`metadata`, la interfaz propone descargar también la portada mediante una
casilla marcada por defecto. Al ejecutar un rango que incluya metadatos se
recorre todo el módulo, aunque no haya ninguna ficha elegible para esa fase, y se
intentan todos los IDs que no tengan un archivo físico de portada. Así, borrar
un archivo permite que se descargue otra vez en la siguiente ejecución. La
descarga sigue siendo una rama opcional y no bloqueante: no condiciona la
catalogación ni la consolidación y puede ejecutarse también sobre fichas
consolidadas. Si Open Library o Google ofrecen imágenes `large` o `medium`,
la selección se limita primero a ese grupo; solo cuando no hay ninguna
utilizable se compara por tamaño de archivo el resto de candidatos, incluida
ISBNdb.

## Arquitectura

- `src/project_meta.py`: metadatos de proyecto/versionado desde `pyproject.toml`.
- `src/backend/main.py`: composición de la aplicación FastAPI y registro de
  enrutadores.
- `src/backend/routers`: rutas de la API separadas por dominio (`core`,
  `ingest`, `workflow`, `books`, `core_books`, `export`, `snapshots`).
- `src/backend/services`: lógica de OCR, metadatos, catálogo, portadas,
  exportación, migraciones e instantáneas.
- `src/backend/schemas`: contratos Pydantic de las cargas JSON de la API.
- `src/frontend`: aplicación Streamlit multipágina y utilidades compartidas.
- `scripts`: inicialización, migraciones, mantenimiento de la base de datos e
  instantáneas.
- `tests`: pruebas de importación, esquema, migraciones, exportación e
  instantáneas.

DuckDB sigue siendo la fuente única de verdad del estado operativo y de la
ficha final.
La política para evolucionar su esquema está documentada en
`docs/MIGRATIONS.md`.

El ciclo operativo `sin ficha -> borrador -> consolidada`, sus bloqueos y la
reapertura explícita están documentados en
[`docs/FORM_LIFECYCLE.md`](docs/FORM_LIFECYCLE.md).

El contrato de las API bibliográficas, la separación entre obra y edición y la
selección de portadas están documentados en
[`docs/METADATA_AND_COVERS.md`](docs/METADATA_AND_COVERS.md).

La página **Datos** permite exportar las fichas de un módulo a un Excel
Unicode, revisarlas fuera de la aplicación e importar sus cambios después de
validarlos y confirmarlos. El procedimiento, los desplegables y las reglas de
formato están documentados en
[`docs/EXTERNAL_REVIEW.md`](docs/EXTERNAL_REVIEW.md).

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
- La ejecución trabaja en el ámbito `block + module`.

### Preparación de imágenes en la extracción

La extracción funciona en dos pasos. Primero analiza el módulo sin modificar
archivos y muestra todos los renombrados, conversiones y borrados propuestos.
Los cambios solo se aplican después de que el usuario revise la lista y los
confirme. Si la carpeta cambia después del análisis, la confirmación se rechaza
y hay que analizarla de nuevo.

- Normaliza nombres reconocibles, por ejemplo `1a1_02.JPG` a
  `01A0001_2.jpg`.
- Convierte HEIC y HEIF a JPEG con orientación EXIF corregida y valida el
  resultado antes de usarlo.
- Permite conservar el original o borrarlo después de validar el JPEG.
- Informa nombres imposibles, colisiones, archivos situados en otro módulo y
  secuencias incompletas o duplicadas para corregirlas a mano.
- Los archivos con incidencias bloqueantes no se indexan. Cuando se convierte
  un HEIC/HEIF válido, el resto del flujo registra y procesa únicamente el JPEG.

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

- `book_items`: estado operativo por elemento, control del flujo de trabajo y
  ciclo de la ficha (`form_status`).
- `book_image_files`: una fila por imagen asociada a cada elemento.
- `book_ocr_data`: texto OCR e ISBN derivados/consolidados.
- `book_bibliographic_sources`: respuesta JSON por proveedor (`google`,
  `openlibrary`, `isbndb`). Open Library guarda la edición exacta y el
  contexto de obra dentro del mismo JSON, sin añadir columnas.
- `books`: tabla principal editable mediante el formulario final.
- `book_field_allowed_values`: valores cerrados para campos del formulario.
- `ref.iso_639_3`: referencia de idiomas ISO 639-3 con `spa_name`.
- `libros_carga_abebooks`: vista de exportación.
- `schema_migrations`: registro de migraciones aplicadas.

## Preparación de recursos locales

Los recursos de `assets/` no se versionan. Se preparan con:

```bash
make prepare-assets
```

Este objetivo se ejecuta automáticamente desde `make setup` y comprueba lo
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

GNU Make es opcional y se mantiene como herramienta de apoyo al desarrollo. Para
los lanzadores solo se necesitan Python 3.12 o posterior y Git.

```bash
python3 scripts/appctl.py setup
python3 scripts/appctl.py launch
```

`setup` crea `.env` desde `.env.example` cuando falta y nunca sobrescribe una
configuración existente. `pyproject.toml` es la única fuente de dependencias; el
detalle de instalación y actualización está en
[`docs/DEPENDENCIES.md`](docs/DEPENDENCIES.md).

También se puede usar `tools/set-up-app.*` una vez y después
`tools/launch-app.*`. El diagnóstico de la instalación está disponible con
`tools/doctor-app.*`. El lanzador normal comprueba `origin/main` en cada
apertura, aplica
automáticamente una actualización estable y continúa con la versión instalada
cuando no hay conexión.

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
- `make install`: reconstruye el entorno y resuelve de nuevo las dependencias
  de `pyproject.toml`.
- `make build`: genera la rueda y el paquete fuente en `dist/`.
- `make update`: aplica el actualizador estable de `appctl`.
- `make start`: actualización automática y arranque estable.
- `make dev`: arranque de desarrollo sin actualización y con recarga.
- `make restart`: detiene y vuelve a arrancar el modo estable.
- `make restart-dev`: detiene y vuelve a arrancar el modo de desarrollo.
- `make dev-back`: solo backend.
- `make dev-front`: solo la interfaz.
- `make init-db`: crea/ajusta esquema de DuckDB mediante migraciones.
- `make migrate-db`: aplica migraciones explícitamente.
- `make db-maint`: mantenimiento ligero de DB.
- `make db-repack`: compacta la base en un archivo nuevo.
- `make db-repack-replace`: compacta la base y reemplaza el archivo original.
- `make publish-snapshot`: publica una instantánea de DuckDB.
- `make list-snapshots`: enumera las instantáneas disponibles.
- `make import-snapshot SNAPSHOT_ID=<id>`: importa una instantánea confirmada.
- `make cleanup-snapshots`: elimina instantáneas antiguas según la política de
  conservación.
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
proveedor local predeterminado; las claves de OpenAI, Google Books e ISBNdb
son opcionales.
La referencia completa, validaciones y resolución de problemas están en
[`docs/CONFIGURATION.md`](docs/CONFIGURATION.md).

Preparación local:

- `APP_ICON_URL`: URL opcional para descargar `assets/dani.png` durante la preparación.

Actualización y arranque:

- `GIT_REMOTE`: remoto del canal estable; por defecto `origin`.
- `GIT_BRANCH`: rama estable; debe ser `main` en instalaciones de usuario.
- `APP_UPDATE_TIMEOUT_SECONDS`: espera máxima de la comprobación remota.
- `APP_STARTUP_TIMEOUT_SECONDS`: espera máxima de salud tras arrancar.
- `APP_UPDATE_BACKUP_DIR`: copias de seguridad locales previas a la
  actualización.
- `APP_UPDATE_BACKUP_KEEP`: número de copias recientes que se conservan.
- `BACK_HOST` / `BACK_PORT`: escucha local del backend.
- `FRONT_HOST` / `FRONT_PORT`: escucha local de la interfaz.

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
- `OCR_OLLAMA_MODEL_SUGGESTIONS`
- `OCR_OPENAI_MODEL`
- `OCR_RESIZE_TO_1800_DEFAULT`
- `OCR_ISBN_OLLAMA_MODEL`
- `OCR_OLLAMA_FALLBACK_MODELS`
- `OCR_USE_SIDECAR`
- `OLLAMA_BASE_URL`
- `LLM_TIMEOUT_SECONDS`
- `OLLAMA_TIMEOUT_SECONDS`

Catalogación automática:

- `CATALOG_PROVIDER` (`ollama` u `openai`)
- `CATALOG_MODEL` (valor compatible usado como respaldo de OpenAI)
- `CATALOG_OLLAMA_MODEL`
- `CATALOG_OPENAI_MODEL`
- `CATALOG_OLLAMA_MODEL_SUGGESTIONS`
- `CATALOG_ARBITER_ENABLED`
- `CATALOG_ARBITER_PROVIDER`
- `CATALOG_ARBITER_MIN_CONFIDENCE`

API y límites:

- `OPENAI_API_KEY`
- `GOOGLE_BOOKS_API_KEY`
- `ISBNDB_API_KEY`
- `OPENLIBRARY_CONTACT`
- `REQUEST_TIMEOUT_SECONDS`
- `WORKFLOW_MAX_ATTEMPTS`
- `GOOGLE_BOOKS_MIN_INTERVAL_SECONDS`
- `OPENLIBRARY_MIN_INTERVAL_SECONDS`

Google Books necesita `GOOGLE_BOOKS_API_KEY` para identificar el proyecto y
aplicar su cuota propia. La búsqueda intenta primero el ISBN; si los operadores
de campo de Google devuelven falsos vacíos, usa títulos obtenidos de las otras
fuentes y solo acepta un resultado cuyo ISBN coincida. Si falta la clave o la
comprobación detecta el fallo de Google sin una coincidencia segura, el
proveedor se registra como error y el proceso continúa con las demás fuentes.
`OPENLIBRARY_CONTACT` permite
incluir un correo de contacto en el `User-Agent`, como recomienda
Open Library para peticiones periódicas. Los intervalos parten de un segundo
entre llamadas, compatibles con el límite público no identificado de Open
Library; se pueden aumentar desde `.env` si fuera necesario.

Open Library consulta por separado la edición exacta de `/isbn/{isbn}.json` y
un contexto reducido de la obra. Para catalogar, editorial, fecha, páginas,
encuadernación e idioma se toman de la edición; sinopsis y materias de la obra
siguen disponibles para título, autoría, categoría, género y palabras clave.
Los payloads históricos agregados se recortan antes de enviarlos al modelo para
que sus listas de editoriales, fechas e ISBN no se interpreten como datos del
ejemplar.

Los lotes del flujo de trabajo no tienen un tiempo de espera global.
`REQUEST_TIMEOUT_SECONDS`
limita cada petición a una API bibliográfica y `LLM_TIMEOUT_SECONDS` limita
cada llamada de OCR o catalogación. Ambos contadores empiezan de nuevo en cada
operación y para cada libro. Si `OLLAMA_TIMEOUT_SECONDS` queda vacío, hereda el
límite general de modelos.

Los enlaces y pasos para crear el proyecto de Google Cloud, habilitar Books API,
restringir la clave y configurar `.env` están en
[`docs/CONFIGURATION.md`](docs/CONFIGURATION.md#configurar-google-books). El
registro de la aplicación, las respuestas recomendadas para el formulario de
Open Library y su configuración local están en
[`docs/CONFIGURATION.md`](docs/CONFIGURATION.md#configurar-open-library).

Interfaz:

- `API_URL`
- `API_TIMEOUT_SECONDS`
- `API_LONG_TIMEOUT_SECONDS`
- `APP_CHANNEL`
- `FRONTEND_THEME_CSS`

## Exportación

La exportación usa la vista `libros_carga_abebooks` y aplica filtros por bloque/módulo.

- Formato: TXT delimitado por TAB, con cabecera.
- Codificación configurable: `windows-1252` (predeterminada) o `utf-8`.
- Ruta de exportación: `GET /export/books/txt`.
- Ruta de exportación por selección: `POST /export/books/txt`.
- Validación no bloqueante: `GET/POST /export/books/validate`.
- Descarga de archivo generado: `GET /export/books/file?filename=...`.

Los campos de la ficha final y la vista exportada se mantienen como contrato
funcional del proyecto.

## Instantáneas

Las instantáneas publican una copia compactada de la base DuckDB en:

```text
<BBDD_DIR>/media-catalog-books/snapshots/
```

Cada instantánea incluye un manifiesto JSON con `snapshot_id`, versión de la
aplicación, versión
real del esquema, origen (`SYNC_ACTOR`/`SYNC_DEVICE`), tamaño y `sha256`.

La importación:

- requiere confirmación explícita (`confirm=true`);
- verifica la suma de comprobación;
- rechaza esquemas desconocidos y bases que no pertenecen a Books;
- valida y migra una copia temporal antes de tocar la base activa;
- crea una copia de seguridad local antes de reemplazar la base de datos;
- restaura la base anterior si falla el registro final;
- actualiza `SYNC_STATE_PATH`.

La actualización automática de la aplicación no importa instantáneas. Sustituir
la base local sigue requiriendo confirmación expresa desde la página de datos.
La operación y sus garantías están detalladas en `docs/SNAPSHOTS.md`.

## Migraciones

El arranque aplica automáticamente las migraciones incrementales pendientes
antes de iniciar los servicios. Cada migración se ejecuta en una transacción y
su suma de comprobación incluye la implementación completa, de modo que una
versión ya
publicada no se puede modificar silenciosamente.

La compatibilidad también se prueba contra una base sintética generada con la
versión `v0.1.1`, verificando que conserva datos, tablas, columnas y el contrato
de exportación. Ver `docs/MIGRATIONS.md` para el procedimiento de desarrollo.

## Lanzadores

La carpeta `tools/` contiene lanzadores de doble clic para Windows y
Ubuntu/Linux:

- preparar la aplicación;
- arrancar la aplicación;
- detener la aplicación;
- actualizar la aplicación.

Ver `tools/README.md`.

## Notas operativas

- No se usan archivos JSON intermedios en disco como mecanismo principal del
  flujo de procesamiento.
- El estado operativo vive en DuckDB.
- La revisión manual y el formulario escriben directamente en base de datos.
- Las migraciones incrementales actualizan el esquema sin alterar los campos de negocio.

## Historial

Para cambios por versión, ver `CHANGELOG.md`.

La política `develop -> main -> tag` y la lista de comprobación para publicar
están
documentadas en `docs/RELEASING.md`.
