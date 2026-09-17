# Roadmap técnico

Este roadmap recoge la alineación de arquitectura, usabilidad y tooling de
`media-catalog-books` respecto a `media-catalog-movies` y `media-catalog-vinyls`.

## Alineación completada

- [x] `P0` Centralizar metadatos de proyecto en `src/project_meta.py`.
- [x] `P0` Exponer la versión de la app desde `pyproject.toml` en FastAPI y Streamlit.
- [x] `P0` Separar `src/backend/main.py` en routers por dominio.
- [x] `P0` Añadir migraciones idempotentes con tabla `schema_migrations`.
- [x] `P0` Convertir el baseline en migraciones incrementales inmutables, con checksum del código y prueba desde una base real de `v0.1.1`.
- [x] `P0` Conectar `scripts/init_db.py` al sistema de migraciones.
- [x] `P0` Añadir `scripts/migrate_db.py`.
- [x] `P0` Añadir snapshots DuckDB con manifiesto, hash y estado de sincronización.
- [x] `P0` Validar identidad y compatibilidad de snapshots, migrar la copia antes de importar y restaurar la base ante fallos finales.
- [x] `P0` Añadir endpoints `/snapshots/*`.
- [x] `P0` Añadir página Streamlit `05_datos` para publicar/importar/limpiar snapshots.
- [x] `P1` Añadir exportación por selección y validación no bloqueante.
- [x] `P1` Normalizar `Makefile` con targets de setup, update, lint, format, test, migraciones y snapshots.
- [x] `P1` Añadir CI con lint y tests.
- [x] `P0` Fijar dependencias reproducibles con hashes y reconstrucción automática del entorno.
- [x] `P1` Validar dependencias, tests y build en Linux y Windows mediante CI.
- [x] `P0` Automatizar `.env` y validar la configuración antes de preparar o arrancar.
- [x] `P1` Ampliar `doctor` con lock, recursos, rutas, DuckDB, puertos y proveedores.
- [x] `P1` Añadir smoke test aislado de FastAPI, Streamlit y DuckDB a CI.
- [x] `P1` Añadir lanzadores en `tools/` para Windows y Linux.
- [x] `P0` Sustituir GNU Make como requisito de usuario por el controlador multiplataforma `appctl`.
- [x] `P0` Separar el arranque estable con autoactualización del modo `dev`.
- [x] `P0` Limitar actualizaciones a fast-forward de `main`, con fallback offline y rollback.
- [x] `P0` Documentar el contrato de publicación `develop -> main -> tag`.
- [x] `P1` Añadir pruebas de API, esquema, migraciones, export y snapshots.
- [x] `P1` Documentar operación en README.
- [x] `P0` Permitir fichas manuales sin ISBN ni catalogación automática y añadir
  consolidación protegida con reapertura explícita.

## Contratos preservados

- [x] `P0` Mantener los campos de la tabla core `books`.
- [x] `P0` Mantener la vista exportada `libros_carga_abebooks`.
- [x] `P0` Mantener el orden y nombres de columnas exportadas.
- [x] `P0` Mantener el flujo funcional de OCR, metadata, catálogo, formulario y exportación.

## Siguientes mejoras posibles

- [ ] `P2` Ampliar tests de frontend con smoke tests de Streamlit si se incorpora una herramienta estable para ello.
- [ ] `P2` Añadir tests de importación completa de snapshots con reinicio de app en entorno aislado.
- [ ] `P2` Revisar si conviene extraer helpers comunes de snapshots entre repos cuando haya una estrategia compartida de paquete interno.
