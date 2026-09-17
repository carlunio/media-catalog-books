# Ciclo de la ficha final

El estado de preparación de una ficha es operativo y se guarda en
`book_items`. No añade ni cambia campos de negocio en `books` y tampoco altera
el contrato de `libros_carga_abebooks` ni de los ficheros exportados.

## Estados

- `not_started`: el item está ingerido, pero todavía no tiene una fila en
  `books`.
- `draft`: existe una ficha editable, creada manualmente o sincronizada desde
  la catalogación automática.
- `consolidated`: la ficha se considera terminada y protegida.

Una ficha consolidada no entra en las colas de OCR, metadata, catálogo o
portada. Su ficha final tampoco puede ser modificada por la API, resincronizada
desde el catálogo ni forzada mediante una ejecución directa del workflow.
Consolidar marca el workflow como terminado y limpia cualquier revisión
pendiente.

## Operación desde el formulario

El selector del formulario incluye todos los items ingeridos. Cuando uno aún
no tiene ficha, `Crear ficha manual` genera un borrador vacío con los mismos
campos y valores predeterminados de la ficha habitual. Esto permite completar
libros que no tengan ISBN o que no hayan pasado por la catalogación automática.

`Consolidar ficha` guarda primero el formulario completo y pide confirmación
antes de protegerlo. `Reabrir ficha` devuelve expresamente una ficha
consolidada a borrador para permitir una corrección. Abrir o guardar el
formulario no resincroniza automáticamente datos del catálogo; esa acción se
mantiene como un botón separado y visible.

## Libros sin ISBN

En una revisión provocada por ausencia de ISBN, la acción `Aceptar que no tiene
ISBN y salir de review` registra `isbn_missing_accepted_at`. El workflow puede
entonces continuar por metadata y catálogo sin volver a la misma revisión. Si
posteriormente se modifica el OCR o se reinicia el item desde OCR, la aceptación
se borra para que el resultado nuevo vuelva a validarse.

La ausencia aceptada de ISBN y el estado de ficha son decisiones distintas: un
libro puede continuar por el workflow o abrirse directamente como ficha manual,
y sólo queda protegido cuando su ficha se consolida.

## Persistencia y migración

La migración `0003_form_lifecycle` añade a `book_items`:

- `form_status`
- `form_consolidated_at`
- `isbn_missing_accepted_at`

Las bases existentes asignan `draft` a los items que ya tienen fila en `books`
y `not_started` al resto. Los snapshots se migran con el mismo mecanismo antes
de sustituir la base local.
