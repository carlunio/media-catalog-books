# Roadmap técnico

Este roadmap recoge la alineación de arquitectura, usabilidad y tooling de
`media-catalog-books` respecto a `media-catalog-movies` y `media-catalog-vinyls`.

## Alineación completada

- [x] `P0` Centralizar metadatos de proyecto en `src/project_meta.py`.
- [x] `P0` Exponer la versión de la app desde `pyproject.toml` en FastAPI y Streamlit.
- [x] `P0` Separar `src/backend/main.py` en routers por dominio.
- [x] `P0` Añadir migraciones idempotentes con tabla `schema_migrations`.
- [x] `P0` Conectar `scripts/init_db.py` al sistema de migraciones.
- [x] `P0` Añadir `scripts/migrate_db.py`.
- [x] `P0` Añadir snapshots DuckDB con manifiesto, hash y estado de sincronización.
- [x] `P0` Añadir endpoints `/snapshots/*`.
- [x] `P0` Añadir página Streamlit `05_datos` para publicar/importar/limpiar snapshots.
- [x] `P1` Añadir exportación por selección y validación no bloqueante.
- [x] `P1` Normalizar `Makefile` con targets de setup, update, lint, format, test, migraciones y snapshots.
- [x] `P1` Añadir CI con lint y tests.
- [x] `P1` Añadir lanzadores en `tools/` para Windows y Linux.
- [x] `P1` Añadir pruebas de API, esquema, migraciones, export y snapshots.
- [x] `P1` Documentar operación en README.

## Contratos preservados

- [x] `P0` Mantener los campos de la tabla core `books`.
- [x] `P0` Mantener la vista exportada `libros_carga_abebooks`.
- [x] `P0` Mantener el orden y nombres de columnas exportadas.
- [x] `P0` Mantener el flujo funcional de OCR, metadata, catálogo, formulario y exportación.

## Siguientes mejoras posibles

- [ ] `P2` Ampliar tests de frontend con smoke tests de Streamlit si se incorpora una herramienta estable para ello.
- [ ] `P2` Añadir tests de importación completa de snapshots con reinicio de app en entorno aislado.
- [ ] `P2` Revisar si conviene extraer helpers comunes de snapshots entre repos cuando haya una estrategia compartida de paquete interno.
