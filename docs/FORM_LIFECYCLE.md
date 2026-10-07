# Ciclo de la ficha final

El estado de preparación de una ficha es operativo y se guarda en
`book_items`. No añade ni cambia campos de negocio en `books` y tampoco altera
el contrato de `libros_carga_abebooks` ni de los ficheros exportados.

## Estados

- `not_started`: el elemento está incorporado, pero todavía no tiene una fila en
  `books`.
- `draft`: existe una ficha editable, creada manualmente o sincronizada desde
  la catalogación automática.
- `consolidated`: la ficha se considera terminada y protegida.

Una ficha consolidada no entra en las colas del flujo principal: OCR,
metadatos y catálogo. Su ficha final tampoco puede modificarse mediante la API,
resincronizarse desde el catálogo ni forzarse mediante una ejecución directa del
flujo de trabajo. Consolidar marca ese flujo como terminado y limpia cualquier
revisión
pendiente.

La descarga de portadas es una rama opcional que depende únicamente de las
imágenes encontradas en las fichas de las API. Cuando una ejecución incluye
metadatos, salvo que se desmarque la opción, recorre todas las fichas con
metadatos
del módulo y procesa las que no tengan un archivo físico de portada. El barrido
también se ejecuta si no había ninguna ficha pendiente de metadatos. Si se
borra
una portada de la carpeta, vuelve a ser candidata aunque la base de datos
conserve temporalmente el estado `downloaded`. Las variantes `large` y
`medium` de Open Library y Google tienen prioridad sobre miniaturas y sobre
ISBNdb; si ninguna está disponible, se conserva la selección por tamaño entre
todos los candidatos restantes. Un fallo de descarga no detiene los metadatos ni
el catálogo. La rama permanece disponible después de consolidar, no exige
reabrir la ficha y solo actualiza el estado y la ruta de la portada; no modifica
el estado de consolidación ni el del flujo principal.

## Operación desde el formulario

El selector del formulario incluye todos los elementos incorporados. Cuando uno
aún
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
ISBN y salir de revisión` registra `isbn_missing_accepted_at`. El flujo de
trabajo puede continuar entonces por metadatos y catálogo sin volver a la misma
revisión. Si posteriormente se modifica el OCR o se reinicia el elemento desde
OCR, la aceptación
se borra para que el resultado nuevo vuelva a validarse.

La ausencia aceptada de ISBN y el estado de ficha son decisiones distintas: un
libro puede continuar por el flujo de trabajo o abrirse directamente como ficha
manual, y solo queda protegido cuando su ficha se consolida.

## Persistencia y migración

La migración `0003_form_lifecycle` añade a `book_items`:

- `form_status`
- `form_consolidated_at`
- `isbn_missing_accepted_at`

Las bases existentes asignan `draft` a los elementos que ya tienen una fila en
`books` y `not_started` al resto. Las instantáneas se migran con el mismo
mecanismo antes
de sustituir la base local.
