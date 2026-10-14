# Formatos de los campos de la ficha

Esta guía recoge los campos cuyo valor necesita una sintaxis, un separador o una
unidad concretos. En el formulario de **MC Books**, el propio nombre de estos
campos abre las instrucciones al pulsarlo. Las cabeceras del Excel de revisión
incluyen la misma ayuda como comentario.

Los campos que no aparecen aquí admiten texto libre o se rellenan mediante un
desplegable. Los desplegables cerrados solo aceptan sus opciones. **Categoría**
y **Género** permiten elegir una sugerencia o escribir un valor nuevo en
español.

## Nombres de persona

Cada persona se escribe como `Apellidos, Nombre`. Se usa una sola coma para
separar los apellidos de los nombres. Cuando haya varias personas, se separan
con punto y coma (`;`).

| Campo | Formato | Ejemplo |
| --- | --- | --- |
| Autor | `Apellidos, Nombre`; varias personas con `;` | `García Márquez, Gabriel; Vargas Llosa, Mario` |
| Traductor | Igual que Autor | `Sáenz, Miguel` |
| Ilustrador | Igual que Autor | `Mendoza, Ana` |
| Editor | Persona responsable de la edición, con el mismo formato | `Rico, Francisco` |
| Fotografía de | Igual que Autor | `Masats, Ramón` |
| Introducción de | Igual que Autor | `Marías, Javier` |
| Epílogo de | Igual que Autor | `Muñoz Molina, Antonio` |

Se admiten apellidos compuestos y partículas, por ejemplo
`de la Cruz, Juana`. No se deben reordenar automáticamente seudónimos,
entidades o nombres cuyo orden no esté claro.

## Valores múltiples y códigos

| Campo | Formato | Ejemplo |
| --- | --- | --- |
| País del autor | Nombre en español, sin duplicados; varios países con `;`, en el orden de los autores | `Colombia; Perú` |
| País de la publicación | Nombre en español; varios países con `;` | `España; Argentina` |\n| Idioma | Nombre en español y en minúsculas; varios idiomas con `;` | `español; francés` |
| Palabras clave | Etiquetas en español separadas por comas; también se aceptan `;` o saltos de línea | `exilio, memoria, París` |
| ISBN | ISBN-10 o ISBN-13 sin espacios ni guiones; se admite `X` final en ISBN-10 | `843760494X` |
| Año de publicación | Cuatro cifras y referido a esta edición | `1987` |

La marca `NOISBN` de **Palabras clave** se añade o elimina automáticamente según
la ficha tenga o no ISBN. En **URL de imágenes**, se escriben los nombres
normalizados y se separan con espacios, por ejemplo
`03B0001.jpg 03B0001_2.jpg 03B0001_3.jpg`.

## Edición, colección y volumen

| Campo | Formato | Ejemplo |
| --- | --- | --- |
| Edición | Valor del desplegable cerrado | `2ª edición` |
| Número de impresión | Valor del desplegable cerrado | `1ª impresión` |
| Nº en la colección | Número entero, sin `nº` ni texto adicional | `24` |
| Volumen | Número romano en mayúsculas, sin `vol.` | `IV` |

Para un intervalo de volúmenes se usa un guion, por ejemplo `I-III`. Para
volúmenes separados se usa una coma, por ejemplo `I, III`. El valor
`OBRA COMPLETA` también está admitido.

## Cantidades, medidas e importes

| Campo | Formato | Unidad o ejemplo |
| --- | --- | --- |
| Nº de páginas | Número entero, sin abreviaturas | `320` |
| Alto | Número entero, sin escribir la unidad | centímetros |
| Ancho | Número entero, sin escribir la unidad | centímetros |
| Fondo | Número entero, sin escribir la unidad | centímetros |
| Peso | Número entero, sin escribir la unidad | gramos |
| Cantidad | Número entero de ejemplares disponibles | `1` |
| Precio | Número sin símbolo de moneda; admite coma o punto decimal | `12,50` |

El precio se normaliza a dos decimales. En el Excel, las cantidades, páginas,
medidas y peso deben seguir siendo valores numéricos; no se deben añadir las
unidades dentro de las celdas.
