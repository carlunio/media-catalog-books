# Hoja de ruta técnica

Esta hoja de ruta recoge la alineación de arquitectura, usabilidad y
herramientas de
`media-catalog-books` respecto a `media-catalog-movies` y `media-catalog-vinyls`.

## Alineación completada

- [x] `P0` Centralizar metadatos de proyecto en `src/project_meta.py`.
- [x] `P0` Exponer la versión de la aplicación desde `pyproject.toml` en
  FastAPI y Streamlit.
- [x] `P0` Separar `src/backend/main.py` en routers por dominio.
- [x] `P0` Añadir migraciones idempotentes con tabla `schema_migrations`.
- [x] `P0` Convertir el esquema base en migraciones incrementales inmutables,
  con suma de comprobación del código y prueba desde una base real de
  `v0.1.1`.
- [x] `P0` Conectar `scripts/init_db.py` al sistema de migraciones.
- [x] `P0` Añadir `scripts/migrate_db.py`.
- [x] `P0` Añadir instantáneas de DuckDB con manifiesto, suma SHA-256 y estado
  de sincronización.
- [x] `P0` Validar la identidad y la compatibilidad de las instantáneas, migrar
  la copia antes de importar y restaurar la base ante fallos finales.
- [x] `P0` Añadir las rutas `/snapshots/*`.
- [x] `P0` Añadir la página de Streamlit `05_datos` para publicar, importar y
  limpiar instantáneas.
- [x] `P1` Añadir exportación por selección y validación no bloqueante.
- [x] `P1` Normalizar `Makefile` con objetivos de preparación, actualización,
  análisis estático, formato, pruebas, migraciones e instantáneas.
- [x] `P1` Añadir integración continua con análisis estático y pruebas.
- [x] `P0` Fijar dependencias reproducibles con hashes y reconstrucción automática del entorno.
- [x] `P1` Validar dependencias, pruebas y construcción en Linux y Windows
  mediante integración continua.
- [x] `P0` Automatizar `.env` y validar la configuración antes de preparar o arrancar.
- [x] `P1` Ampliar `doctor` con el archivo de bloqueo, recursos, rutas, DuckDB,
  puertos y proveedores.
- [x] `P1` Añadir a la integración continua una prueba de humo aislada de
  FastAPI, Streamlit y DuckDB.
- [x] `P1` Añadir lanzadores en `tools/` para Windows y Linux.
- [x] `P0` Sustituir GNU Make como requisito de usuario por el controlador multiplataforma `appctl`.
- [x] `P0` Separar el arranque estable con autoactualización del modo `dev`.
- [x] `P0` Limitar las actualizaciones a avances directos (`fast-forward`) de
  `main`, con funcionamiento sin conexión y reversión.
- [x] `P0` Documentar el contrato de publicación `develop -> main -> tag`.
- [x] `P1` Añadir pruebas de API, esquema, migraciones, exportación e
  instantáneas.
- [x] `P1` Documentar la operación en el README.
- [x] `P0` Permitir fichas manuales sin ISBN ni catalogación automática y añadir
  consolidación protegida con reapertura explícita.
- [x] `P0` Preparar imágenes en dos pasos, con conversión segura de HEIC/HEIF,
  revisión de nombres y confirmación previa de los cambios.
- [x] `P0` Separar la descarga de portadas del flujo principal y mantenerla
  disponible para fichas consolidadas.
- [x] `P1` Adaptar Google Books y Open Library a sus contratos actuales,
  distinguir obra y edición sin ampliar el esquema y registrar el método de
  búsqueda de Google.
- [x] `P1` Barrer las portadas ausentes por archivo físico y priorizar las
  variantes `large` y `medium` de Open Library y Google.
- [x] `P0` Añadir la revisión externa de fichas mediante Excel Unicode, con
  listas cerradas y ampliables, validación previa, confirmación, control de
  conflictos e importación atómica.
- [x] `P1` Simplificar las instalaciones para usar `pyproject.toml` como única
  fuente de dependencias en Linux, Windows y CI.

## Contratos preservados

- [x] `P0` Mantener los campos de la tabla principal `books`.
- [x] `P0` Mantener la vista exportada `libros_carga_abebooks`.
- [x] `P0` Mantener el orden y nombres de columnas exportadas.
- [x] `P0` Mantener el flujo funcional de OCR, metadatos, catálogo, formulario
  y exportación.

## Siguientes mejoras posibles

- [ ] `P1` Introducir adaptadores normalizados y versionados para que la
  catalogación no dependa directamente de la estructura cambiante de cada API.
- [ ] `P1` Añadir contratos de respuesta y datos de prueba representativos para
  cada proveedor bibliográfico.
- [ ] `P2` Ampliar las pruebas de la interfaz con pruebas de humo de Streamlit
  cuando exista una herramienta estable para ello.
- [ ] `P2` Añadir pruebas de importación completa de instantáneas con reinicio
  de la aplicación en un entorno aislado.
- [ ] `P2` Revisar si conviene extraer utilidades comunes de instantáneas entre
  repositorios cuando exista una estrategia compartida de paquete interno.
