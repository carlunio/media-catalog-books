# Migraciones de base de datos

Las migraciones mantienen cada base DuckDB local compatible con la versión de
la aplicación sin sustituir datos del usuario ni modificar el contrato de la
ficha final o de la exportación.

## Organización

- `src/backend/services/migrations.py`: registro ordenado y ejecutor.
- `src/backend/migrations/versions/`: implementaciones inmutables por versión.
- `schema_migrations`: historial guardado dentro de cada base.
- `tests/fixtures/books-v0.1.1.duckdb.gz`: base sintética creada con la versión
  `v0.1.1` para comprobar actualizaciones desde una versión publicada real.

Historial actual:

1. `0001_baseline`: reproduce el esquema publicado en `v0.1.0`.
2. `0002_v0_1_1`: añade `workflow_action` y el formato publicado del precio de
   `v0.1.1`.
3. `0003_form_lifecycle`: añade a `book_items` el estado operativo de la
   ficha,
   la fecha de consolidación y la aceptación explícita de la ausencia de ISBN.
4. `0004_optional_cover_branch`: da por terminados los flujos de trabajo antiguos
   que ya habían catalogado el libro y solo esperaban la portada.

La tercera y la cuarta migración no modifican los campos de negocio de `books`
ni las columnas de `libros_carga_abebooks`. Las fichas que ya existen se migran
a `draft`; los elementos sin ficha quedan en `not_started`. La cuarta conserva
intacto `cover_status`, de modo que la portada sigue pendiente en su rama
opcional aunque deje de bloquear el flujo principal.

## Garantías del ejecutor

- Aplica las migraciones pendientes en orden y dentro de una transacción por
  migración.
- Registra la versión, el nombre, la versión de la aplicación y la suma de
  comprobación de la implementación.
- Rechaza versiones desconocidas, registros duplicados, orden incorrecto y
  cambios en una migración ya aplicada.
- Admite la suma de comprobación histórica del antiguo esquema base y la
  actualiza de forma
  controlada al formato que valida el código completo.
- Se puede ejecutar varias veces: una base actualizada no vuelve a cambiar.

`python3 scripts/appctl.py launch`, `python3 scripts/init_db.py` y
`python3 scripts/migrate_db.py` aplican este mismo registro. El arranque se
detiene si la base no puede actualizarse de forma segura.

La importación de instantáneas también usa este ejecutor sobre una copia
temporal;
la base activa solo se sustituye cuando la copia ya está en la versión actual.

## Añadir una migración

1. Crear un módulo nuevo en `src/backend/migrations/versions/`, con el siguiente
   número correlativo y una función `apply(con)` idempotente.
2. Añadirlo al final de `MIGRATIONS` en
   `src/backend/services/migrations.py`.
3. No editar módulos ya publicados. La suma de comprobación provocará un error
   deliberado
   si su implementación cambia.
4. Añadir pruebas de base nueva, actualización desde la versión anterior,
   segunda ejecución sin cambios y reversión ante un fallo.
5. Cuando cambie una versión compatible, conservar una base de prueba sintética
   creada por esa versión; nunca usar una base con inventario real.

La migración debe limitarse a cambios técnicos de esquema y transformación de
los datos imprescindibles. Los campos de negocio y la vista exportada solo
pueden
cambiar mediante una decisión funcional explícita.

## Comprobación manual

```bash
python3 scripts/migrate_db.py
make test
```

No se deben insertar, borrar ni corregir filas de `schema_migrations`
manualmente.
