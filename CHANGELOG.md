# Registro de cambios

## [Unreleased]

### Añadido

- Preparación de imágenes en dos pasos: análisis sin cambios, listado de
  renombrados, conversiones y borrados, y confirmación explícita con detección
  de cambios posteriores en la carpeta.
- Conversión segura de HEIC/HEIF a JPEG con orientación EXIF, validación del
  resultado y borrado opcional del original.
- Detección previa de nombres imposibles, colisiones, módulos incoherentes y
  secuencias de imágenes incompletas o duplicadas.
- Guía operativa de metadatos y portadas en
  `docs/METADATA_AND_COVERS.md`.
- Instrucciones verificadas para crear y restringir una clave de Google Books,
  registrar el uso de las API de Open Library y configurar ambas integraciones.
- Revisión externa de fichas desde la página Datos mediante Excel Unicode
  (`.xlsx`), con filtros por módulo y estado, cabeceras y colores del formulario,
  desplegables cerrados o ampliables según el campo y guía operativa propia.
- Vista previa de todos los cambios antes de importar, validación de formatos y
  conflictos, confirmación explícita, guardado atómico y regeneración automática
  de la descripción. Las fichas pueden conservar su estado o consolidarse tras
  la importación.

### Cambiado

- `pyproject.toml` pasa a ser la única declaración de dependencias. La
  instalación y la integración continua resuelven directamente sus intervalos
  compatibles; se eliminan `requirements.lock`, `pip-tools` y los comandos de
  mantenimiento del lock. `appctl` reconstruye el entorno cuando cambia la
  declaración y permite forzarlo mediante `setup --force` o `make install`.
  Las instalaciones con el controlador anterior requieren una actualización
  manual única antes de recuperar el flujo automático.
- La descarga de portadas pasa a ser una rama opcional que depende de las fichas
  de las API. Toda ejecución que incluya metadatos recorre el módulo completo,
  incluso con cero fichas pendientes de esa fase, y reintenta los ID cuyo
  archivo físico se haya borrado aunque la base conserve
  `cover_status=downloaded`. La casilla permite desactivar el barrido y sus
  fallos no detienen el flujo de procesamiento.
- Las variantes `large` y `medium` de Open Library y Google se eligen antes
  de comparar miniaturas o imágenes de ISBNdb. La rama de portadas también puede
  ejecutarse sobre fichas consolidadas sin reabrirlas ni alterar su bloqueo.
- Open Library obtiene por separado la edición exacta por ISBN y un contexto
  reducido de la obra, guardados dentro del mismo JSON. El catálogo usa los
  datos de edición para la publicación y conserva la sinopsis y las materias de
  la obra para la categoría y el género, sin crear columnas nuevas.
- Google Books admite una clave propia del proyecto, enviada mediante una
  cabecera, y añade una búsqueda alternativa por título que siempre se valida
  contra el ISBN exacto cuando sus operadores de campo devuelven falsos vacíos.
  Cada ficha registra si se obtuvo directamente por ISBN o mediante la
  alternativa validada.
- Los intervalos predeterminados de Google Books y Open Library bajan de 60 a 1
  segundo. Los errores HTTP se guardan sin exponer credenciales.

### Corregido

- La prueba del actualizador crea de forma explícita la rama `main` en su remoto
  temporal y ya no depende de la rama inicial configurada en el sistema.
- Se revisan la ortografía española y la terminología de la documentación, la
  interfaz y los mensajes operativos.
- La sobrescritura del flujo de trabajo solo repite la etapa elegida y las
  posteriores que sigan activas; siempre excluye los libros terminados, en
  ejecución, pendientes de revisión o con la ficha consolidada. La interfaz
  limita el lote al número real de elementos elegibles.
- El redimensionado de imágenes a 1800 px para OCR queda desactivado de forma
  predeterminada y se mantiene disponible como opción manual.
- Los resultados del flujo de trabajo muestran el estado individual de Google
  Books, Open Library e ISBNdb. Google reintenta las respuestas transitorias
  429/5xx y distingue explícitamente una respuesta vacía de un error de llamada.
- Los lotes dejan de tener un tiempo de espera HTTP global. Las API
  bibliográficas y cada llamada de OCR o catalogación tienen límites
  independientes que se reinician en cada operación; un tiempo de espera
  agotado en Ollama no bloquea el resto del lote.

## [1.0.0] - 2026-09-17

### Añadido

- Controlador multiplataforma `scripts/appctl.py` para preparar, arrancar,
  actualizar, detener y diagnosticar la instalación sin depender de GNU Make.
- Lanzador de diagnóstico para Windows y Linux.
- Actualización automática del canal estable al abrir la aplicación, con copia
  de seguridad previa de DuckDB, comprobación de salud y reversión del código y
  de los datos.
- Política de publicación `develop -> main -> tag` documentada en
  `docs/RELEASING.md`.
- Metadatos del proyecto centralizados en `src/project_meta.py` y versión
  visible en FastAPI y Streamlit.
- Enrutadores de FastAPI separados por dominio para núcleo, ingesta, flujo de
  trabajo, libros, ficha principal, exportación e instantáneas.
- Migraciones idempotentes con la tabla `schema_migrations` y el script
  `scripts/migrate_db.py`.
- Base de prueba sintética creada con `v0.1.1` para verificar actualizaciones
  desde una versión publicada.
- Validación de identidad y compatibilidad de las instantáneas antes de
  importarlas, con migración aislada de la copia y reversión completa.
- Guía técnica de instantáneas en `docs/SNAPSHOTS.md`.
- Instantáneas de DuckDB con manifiesto, verificación SHA-256, copia de
  seguridad local antes de importar y página de Streamlit `05_datos`.
- Objetivos de Make para migraciones, instantáneas, actualización, análisis
  estático, formato y pruebas.
- Integración continua con análisis estático y pruebas, y lanzadores de
  `tools/` para Windows y Linux.
- Pruebas de importación de la API, esquema, migraciones, exportación e
  instantáneas.
- Archivo de bloqueo reproducible con sumas de comprobación, construcción PEP
  517 y guía de mantenimiento de dependencias.
- Creación automática y no destructiva de `.env`, validación de la
  configuración y prueba de humo aislada.
- Ciclo operativo de ficha `not_started -> draft -> consolidated`, con
  creación manual para cualquier libro incorporado y reapertura explícita.
- Aceptación persistente de libros sin ISBN para que puedan continuar hacia los
  metadatos, la catalogación y el formulario sin volver a revisión.

### Cambiado

- Los lanzadores de `tools/` delegan en `appctl`; GNU Make queda como
  herramienta opcional para el desarrollo.
- `launch` funciona como arranque estable sin recarga; `dev` mantiene la
  recarga y nunca actualiza Git.
- Las actualizaciones solo aceptan avances directos (`fast-forward`) desde una
  instalación limpia situada en la rama estable.
- `scripts/init_db.py` pasa a aplicar migraciones en lugar de inicializar el
  esquema directamente.
- El esquema base monolítico pasa a un historial incremental inmutable; cada
  suma de comprobación cubre la implementación completa y cada paso se aplica
  en una transacción.
- La creación del esquema sale del servicio de libros y queda aislada en
  módulos de migración versionados.
- Los manifiestos de las instantáneas usan la versión real del registro de
  migraciones y distinguen integridad, compatibilidad e importabilidad.
- La pantalla de Datos muestra el esquema de cada instantánea y las migraciones
  aplicadas durante la importación.
- `src/backend/main.py` queda como composición de la aplicación y sus
  enrutadores.
- El README incorpora la operación, las instantáneas, las herramientas y la
  hoja de ruta técnica.
- La instalación pasa a ser reproducible desde `requirements.lock`, con
  reconstrucción automática del entorno cuando cambia.
- La integración continua se amplía a Ubuntu y Windows para validar
  dependencias, pruebas y empaquetado.
- La integración continua y `make lint` comprueban tanto Ruff como el formato
  de Black.
- `doctor` revisa el remoto Git, el archivo de bloqueo, los recursos, los
  permisos, DuckDB, los puertos y los proveedores sin modificar datos.
- Las fichas consolidadas quedan fuera de las colas del flujo de trabajo y
  protegidas frente a edición, resincronización y ejecuciones automáticas hasta
  su reapertura.
- El formulario deja de sincronizar implícitamente la ficha al abrir o guardar;
  la sincronización desde la catalogación automática se convierte en una acción
  expresa.

### Corregido

- La versión expuesta por `pyproject.toml`, FastAPI y Streamlit vuelve a
  coincidir con la versión publicada (`1.0.0`).
- Los recursos locales dejan de proponerse para su versionado: `make setup`
  descarga la tabla ISO 639-3 oficial y permite preparar el icono opcional
  mediante `APP_ICON_URL`.
- La distribución de idiomas pasa a ser `python-iso639` y
  `langcodes[data]`, que corresponden a las API utilizadas.
- El proveedor predeterminado del catálogo queda alineado con `.env.example`
  y con la interfaz: Ollama.
- La ejecución manual de metadatos deja de enviar parámetros ajenos a su
  contrato que provocaban un error en la ruta de la API.

### Conservado

- No se introducen cambios intencionados en los campos de negocio de `books`
  ni en las columnas de `libros_carga_abebooks`.

## [0.1.1] - 2026-03-23

### Añadido

- Trazabilidad de la ejecución mediante la nueva columna `workflow_action` de
  `book_items`.
- Visualización del LLM activo por elemento en ejecución (proveedor y modelo) en
  la pantalla de orquestación.
- Contrato explícito en el prompt de catalogación para los campos de persona:
  - formato `Apellido(s), Nombre(s)`;
  - soporte de apellidos compuestos;
  - separación de personas por `;` mediante una lista JSON.
- Estructura de carpetas de portadas por bloque (`A`, `B`, `C`) en
  `data/output/covers`.

### Cambiado

- El campo `price` de la vista `libros_carga_abebooks` se exporta como texto
  con formato `XX.XX €`.
- La salida de portadas se reorganiza como
  `data/output/covers/<BLOQUE>/<MODULO>/<BOOK_ID>.<ext>`.
- `seed_widget_once` mantiene la coherencia de los valores predeterminados con
  `.env` al navegar entre páginas.
- El selector de modelos de Ollama usa automáticamente el modelo preferido
  cuando el valor actual no es válido.
- Streamlit sustituye `use_container_width` por `width="stretch"`.

### Corregido

- Se corrigen las desviaciones de los valores predeterminados de proveedor y
  modelo al cambiar de página.
- La descarga de portadas solo se omite cuando `cover_path` existe físicamente.
- `.gitignore` mantiene únicamente la estructura de carpetas de portadas en
  Git y excluye su contenido generado.

### Interno

- Integración de `develop` en `main` para publicar `v0.1.1`.

## [0.1.0] - 2026-03-22

Primera versión del proyecto nuevo `media-catalog-books` después de reconstruir
`book_catalog` v0.3. La arquitectura, el flujo de trabajo y la persistencia son
nuevos, pero mantienen el objetivo funcional de catalogar libros.

### Añadido

- Arquitectura de servidor e interfaz:
  - backend en FastAPI con rutas para ingesta, OCR, metadatos, catalogación,
    descarga de portadas, revisión manual, sincronización de la ficha final y
    exportación;
  - interfaz multipágina en Streamlit con el flujo operativo completo.
- Orquestación por etapas mediante LangGraph
  (`ocr -> metadata -> catalog -> cover`) con ejecución por lotes.
- Estado persistente del flujo por elemento, con transiciones explícitas entre
  etapas y revisión manual.
- DuckDB como fuente única de verdad.
- Scripts dedicados para inicializar la base de datos (`init_db`) y realizar
  tareas de mantenimiento (`db_maintenance`, compactación y `VACUUM`).
- Modelo de datos orientado a producción:
  - tabla de elementos del flujo;
  - tabla de imágenes por elemento, con varias imágenes por artículo;
  - tabla de OCR/ISBN;
  - tabla de respuestas de fuentes bibliográficas;
  - tabla principal `books` para la catalogación final;
  - tabla de valores cerrados para campos del formulario;
  - esquema `ref` para datos auxiliares;
  - vista de exportación `libros_carga_abebooks`.
- OCR con proveedores configurables (`ollama` y `openai`) y soporte
  específico para modelos locales.
- Extracción y validación de ISBN con normalización, limpieza y comprobación de
  validez.
- Integración de Google Books, Open Library e ISBNdb, con persistencia de las
  fichas en DuckDB.
- Catálogo automático mediante LLM con prompts y reglas adaptadas a libros
  antiguos y de segunda mano.
- Revisión manual de control de calidad para OCR/ISBN y para la ficha final.
- Formulario final en Streamlit inspirado en el formulario histórico de Access,
  con disposición por bloques, campos cerrados y libres, edición manual y
  guardado en `books`.
- Generación automática de una descripción comercial a partir de los campos de
  la ficha.
- Exportación TXT delimitada por tabuladores, con cabecera y opciones de
  codificación, incluida `windows-1252`.
- Comandos de desarrollo y operación en `Makefile`, compatibles con Windows y
  Ubuntu.
- Estructura normalizada de proyecto y datos: `data/input` por bloques y
  módulos, `data/output`, recursos y utilidades.

### Cambiado

- El procesamiento deja de depender de carpetas de etapa y archivos JSON
  intermedios y pasa a un flujo orquestado con estado persistente en la base de
  datos y API de servicio.
- La ejecución deja de ser una secuencia de scripts independientes y pasa a un
  flujo controlado por etapas, con reintentos y revisión.
- Se separan las responsabilidades entre los servicios del backend
  (procesamiento y persistencia) y la interfaz (operación y revisión).
- La configuración de proveedores, modelos LLM y etapas se unifica en `.env`.
- Las ejecuciones quedan limitadas por el ámbito de bloque y módulo para evitar
  mezclar inventarios.
- Se añaden controles para entornos reales: limitación del ritmo por proveedor,
  mantenimiento de la base de datos y limpieza de trazas prescindibles.

### Eliminado

- Dependencia operativa del esquema heredado de `book_catalog_v0.3`.
- Archivos JSON como almacenamiento intermedio principal.
- Access como herramienta principal para editar la ficha final.
- Rutas rígidas ligadas a una máquina concreta.

### Corregido

- Incoherencias de estado entre etapas.
- Integración OCR/ISBN para admitir salidas imperfectas y validación posterior.
- Flujo de revisión manual para consolidar cambios de forma controlada.
- Compatibilidad de la exportación con sistemas que requieren un formato de
  texto específico.

### Notas de migración

Esta versión inicia el sistema nuevo y no presupone una migración automática de
los datos históricos de `book_catalog_v0.3`.

- El estado operativo y los datos intermedios se guardan en DuckDB.
- Para iniciar un entorno limpio:
  1. crear el entorno e instalar las dependencias;
  2. configurar `.env`;
  3. ejecutar el script de inicialización de la base de datos;
  4. iniciar el backend y la interfaz mediante `make`.
- El inventario debe estar bajo `data/input/<BLOQUE>/<MODULO>/...`.
- La exportación final debe hacerse desde la vista
  `libros_carga_abebooks`, con el filtro de bloque y módulo correspondiente al
  lote de salida.
