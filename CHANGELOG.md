## [0.1.1] - 2026-03-23

### Added
- Trazabilidad de ejecución en workflow con nueva columna `workflow_action` en `book_items`.
- Visualización del LLM activo por ítem en ejecución (provider/model) en la pantalla de orquestación.
- Contrato explícito en el prompt de catalogación para campos de persona:
  - formato `Apellido(s), Nombre(s)`
  - soporte de apellidos compuestos
  - separación de personas por `;` (vía lista JSON).
- Estructura de carpetas de portadas versionada por bloque (`A`, `B`, `C`) en `data/output/covers`.

### Changed
- Exportación: el campo `price` de la vista `libros_carga_abebooks` ahora sale como texto con formato `XX.XX €`.
- Descarga de portadas: salida reorganizada a `data/output/covers/<BLOQUE>/<MODULO>/<BOOK_ID>.<ext>`.
- Sincronización de defaults en frontend: mejora de `seed_widget_once` para mantener coherencia con `.env` al navegar entre páginas.
- Selector de modelos Ollama: fallback automático al modelo preferido cuando el valor actual no es válido.
- Compatibilidad Streamlit: reemplazo de `use_container_width` por `width="stretch"`.

### Fixed
- Corrección de drift de valores por defecto (provider/model) en frontend tras cambios de página.
- Corrección de “falsos skip” en covers: solo se omite descarga si `cover_path` existe físicamente.
- Ajustes de `.gitignore` para mantener solo estructura de carpetas de `covers` en Git y excluir contenido generado.

### Internal
- Integración de `develop` en `main` para publicar `v0.1.1`.



## [0.1.0] - 2026-03-22

Primera versión del nuevo proyecto `media-catalog-books`.  
Esta release es la primera versión estable después de reconstruir el antiguo proyecto `book_catalog` (v0.3): es una reconstrucción completa de arquitectura, flujo de trabajo y persistencia que mantiene el objetivo funcional de catalogación de libros con base técnica nueva.

### Added

- Nueva arquitectura `backend + frontend`:
- Backend en FastAPI con endpoints para ingesta, OCR, metadata, catalogación, descarga de cubiertas, revisión manual, sincronización de ficha final y exportación.
- Frontend multipágina en Streamlit con flujo de trabajo operativo completo.
- Orquestación por etapas mediante LangGraph (`ocr -> metadata -> catalog -> cover`) con ejecución por lotes.
- Gestión de estado de workflow por item, con transiciones explícitas entre etapas y soporte de revisión manual.
- Persistencia centralizada en DuckDB como fuente única de verdad.
- Inicialización de base de datos con scripts dedicados (`init_db`) y utilidades de mantenimiento (`db_maintenance`, repack/vacuum).
- Modelo de datos orientado a producción:
- Tabla de items de workflow.
- Tabla de imágenes por item (soporte nativo para múltiples imágenes por artículo).
- Tabla de OCR/ISBN.
- Tabla de respuestas de fuentes bibliográficas.
- Tabla core `books` para la catalogación final.
- Tabla de valores cerrados para campos de formulario.
- Esquema `ref` para datos de apoyo.
- Vista de exportación `libros_carga_abebooks`.
- Proceso de OCR con proveedores configurables (`ollama` y `openai`) y soporte específico para modelos OCR locales.
- Extracción y validación de ISBN integrada (normalización, limpieza y chequeo de validez).
- Integración de APIs bibliográficas (Google Books, OpenLibrary, ISBNdb) con persistencia de fichas en DuckDB.
- Catálogo automático por LLM con prompts y reglas adaptadas al dominio de libro antiguo/segunda mano.
- Revisión manual tipo “control de calidad” para OCR/ISBN y para ficha final.
- Formulario final de catalogación en Streamlit inspirado en el formulario histórico de Access (disposición por bloques, campos cerrados y libres, edición manual y guardado sobre `books`).
- Generación automática de descripción comercial a partir de campos de ficha.
- Exportación en TXT delimitado por tabuladores, con cabecera y opciones de codificación (incluyendo `windows-1252` para compatibilidad con flujos externos).
- Comandos de desarrollo y operación en `Makefile` (`dev`, `stop`, `init-db`, `db-maint`, `db-repack`, etc.) con compatibilidad práctica para Windows/Ubuntu.
- Estructura de proyecto y datos estandarizada (`data/input` por bloques y módulos, `data/output`, assets y utilidades).

### Changed

- Cambio total de paradigma de procesamiento:
- De pipeline basado en carpetas de etapa y JSON intermedio (v0.3),
- A pipeline orquestado con estado persistido en BBDD y APIs de servicio.
- La lógica de negocio deja de depender de ficheros intermedios como mecanismo principal de coordinación.
- La ejecución deja de ser una secuencia de scripts sueltos y pasa a un workflow controlado por etapas, con posibilidad de reintento y revisión.
- Se separan responsabilidades entre servicios backend (procesamiento/persistencia) y frontend (operación/revisión).
- Se unifica la configuración por `.env` para proveedores/modelos LLM y comportamiento de etapas.
- Se adopta enfoque “scope-aware” por bloque/módulo para limitar ejecuciones y evitar mezclar inventarios.
- Se incorporan controles de operación para entornos reales (throttling por proveedor, mantenimiento de DB, limpieza de trazas no esenciales).

### Removed

- Dependencia operativa del esquema legacy de `book_catalog_v0.3` como fuente de ejecución.
- Dependencia de JSON como almacenamiento intermedio principal del pipeline.
- Dependencia de Access como herramienta de edición principal de la ficha final (se migra el trabajo de formulario a Streamlit + DuckDB).
- Suposiciones rígidas de ruta por máquina para datos de trabajo (se avanza hacia rutas parametrizadas por entorno).

### Fixed

- Correcciones estructurales de flujo para evitar incoherencias de estado entre etapas.
- Correcciones de robustez en integración OCR/ISBN para soportar casos de salida imperfecta y validación posterior.
- Correcciones en flujo de revisión manual para consolidar cambios en base de datos de forma controlada.
- Correcciones de compatibilidad de exportación con sistemas que requieren formato de texto específico.

### Migration Notes

- Esta versión se considera arranque del sistema nuevo: no presupone migración automática de datos históricos desde `book_catalog_v0.3`.
- El estado operativo y los intermedios deben considerarse DuckDB-first.
- Para inicializar entorno limpio:
- Crear entorno y dependencias.
- Configurar `.env`.
- Ejecutar script de init de DB.
- Iniciar backend/frontend con `make`.
- El inventario debe organizarse bajo `data/input/<BLOQUE>/<MODULO>/...` para ser detectado y procesado correctamente.
- La exportación final debe hacerse desde la vista `libros_carga_abebooks`, aplicando filtro por bloque/módulo según lote de salida.
