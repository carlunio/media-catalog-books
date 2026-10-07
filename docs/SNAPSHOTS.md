# Instantáneas de DuckDB

Las instantáneas permiten trasladar el estado completo de Media Catalog Books
entre instalaciones. Cada publicación contiene un fichero DuckDB y un
manifiesto JSON con origen, versión de la aplicación, versión de esquema,
tamaño y suma SHA-256.

## Compatibilidad

El listado separa tres conceptos:

- `valid`: existen el manifiesto y la base, y la suma SHA-256 coincide.
- `compatible`: esta versión de la aplicación conoce el esquema declarado.
- `importable`: la instantánea es válida y compatible.

Una instantánea puede estar en estado `current`, `upgrade_required` o
`incompatible`. El valor histórico `schema_version: "1"` se admite como legado
y se actualiza mediante las migraciones incrementales actuales. Una versión
futura o
desconocida se muestra en el listado, pero no se puede seleccionar para
importarla.

El manifiesto es un primer filtro. La base también se inspecciona directamente
durante la importación para impedir que un manifiesto incorrecto permita cargar
una base de otra aplicación o con migraciones desconocidas.

## Secuencia de importación

1. Exige confirmación explícita y vuelve a comprobar el SHA-256 del original.
2. Copia la instantánea a un archivo temporal junto a la base local.
3. Comprueba que es una base DuckDB legible y contiene las relaciones propias
   de Media Catalog Books.
4. Aplica a la copia todas las migraciones pendientes y valida otra vez el
   esquema.
5. Crea una copia de seguridad de la base local en `data/backups/local`.
6. Sustituye la base mediante un renombrado atómico y registra la importación.
7. Si falla el registro final, restaura automáticamente la base anterior.

La instantánea publicada nunca se modifica: las migraciones se aplican únicamente
a la copia temporal. La respuesta API y el estado local registran la versión de
origen, la versión final y las migraciones ejecutadas.

Después de importar se debe reiniciar la aplicación para cerrar cualquier
operación que todavía pudiera conservar una conexión a la base anterior.

## Publicación

Antes de publicar, la base local se migra al esquema actual. El manifiesto usa
la versión devuelta por el registro de migraciones, no un número independiente.
Esto impide publicar como actual una base que la propia aplicación no puede
validar.

## Comandos

```bash
python3 scripts/snapshots.py status
python3 scripts/snapshots.py list
python3 scripts/snapshots.py publish
python3 scripts/snapshots.py import <snapshot_id> --confirm
python3 scripts/snapshots.py cleanup
```

No se deben editar manualmente los manifiestos ni los ficheros DuckDB
publicados: cualquier cambio invalida el SHA-256.
