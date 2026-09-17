# Publicación de versiones

## Contrato de ramas

- `develop` contiene el trabajo en curso y puede no estar listo para usuarios.
- `main` contiene exclusivamente la última versión estable publicada.
- Cada integración en `main` debe actualizar la versión de `pyproject.toml`,
  cerrar la entrada correspondiente de `CHANGELOG.md` y crear un tag
  `vX.Y.Z`.
- No se realizan commits de desarrollo directamente en `main`.
- La integración `develop -> main` debe ser revisada y pasar CI antes de
  publicarse.

Los equipos de usuario permanecen en `main`. El lanzador estable sólo acepta
avances fast-forward desde `origin/main`; nunca mezcla ramas ni sobrescribe
cambios locales.

## Lista de publicación

1. Confirmar que el árbol de `develop` está limpio.
2. Ejecutar `make check-lock`, `make lint`, `make test`, `make smoke` y `make build`.
   `make lint` comprueba tanto Ruff como el formato de Black.
3. Ejecutar `python3 scripts/appctl.py doctor` y revisar todos sus avisos.
4. Actualizar la versión en `pyproject.toml`.
5. Mover los cambios de `Unreleased` a la nueva versión en `CHANGELOG.md`.
6. Integrar `develop` en `main` mediante pull request.
7. Verificar CI en `main`.
8. Crear y publicar el tag anotado `vX.Y.Z`.
9. Comprobar `appctl launch` desde un clon limpio de `main` en Windows y Linux.

## Compatibilidad del actualizador

El protocolo de `appctl` debe seguir siendo compatible con la versión
inmediatamente anterior. Una actualización:

- detiene primero la instancia gestionada;
- comprueba rama y árbol de trabajo;
- descarga únicamente `origin/main`;
- exige que el cambio sea fast-forward;
- crea un backup local de DuckDB;
- instala dependencias sólo si cambia su declaración;
- aplica migraciones y comprueba la salud de ambos servicios;
- restaura código y base anterior si la nueva versión no arranca.

La ausencia de conexión no bloquea el arranque. Los snapshots de datos no se
importan automáticamente porque esa operación sustituye la base local.
