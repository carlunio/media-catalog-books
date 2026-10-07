# Revisión externa de fichas en Excel

La página **Datos** permite sacar fichas de la aplicación para que otra persona
las revise con los libros delante y devolver después las correcciones. El
intercambio usa el formato estándar `.xlsx` (OOXML), que conserva texto Unicode.
La codificación `windows-1252` continúa reservada exclusivamente para el TXT de
AbeBooks.

## Exportar

1. Abre **Datos > Revisión externa en Excel**.
2. Elige el bloque, el módulo y el estado de las fichas:
   - **Borradores**;
   - **Consolidadas**;
   - **Todas las fichas existentes**.
3. Pulsa **Preparar Excel de revisión**.
4. Descarga el archivo preparado.

Solo se incluyen registros que ya tienen ficha en la tabla principal. Las
referencias sin ficha todavía deben crearse o sincronizarse desde el formulario.

La hoja **Fichas** contiene las mismas cabeceras y colores del formulario. Las
dos primeras columnas identifican el registro y su estado. Las columnas grises
son informativas; las blancas y rojas son editables. La descripción también es
informativa porque se vuelve a generar al importar.

El archivo incluye otras hojas internas con los valores originales, las listas
permitidas y la versión de la plantilla. No se deben borrar ni modificar. La
hoja **Instrucciones** resume las reglas para la persona que revisa.

## Desplegables

Los campos siguientes usan listas cerradas y solo admiten valores del
desplegable:

- tipo de artículo;
- estado de stock y estado de carga;
- edición y número de impresión;
- información sobre ilustraciones;
- encuadernación;
- estado de conservación y estado de la cubierta;
- dedicatorias;
- plantilla de envío;
- catálogos 1, 2 y 3.

**Categoría** y **Género** muestran también un desplegable, pero admiten valores
nuevos. Esta es la misma distinción que aplica el formulario de la aplicación.

Los valores históricos que ya estaban presentes al exportar se conservan en las
listas para que una ficha antigua no quede invalidada solo por mantener su valor
actual.

## Formatos

Las referencias, los ISBN y los años se escriben como texto para evitar que
Excel elimine ceros iniciales o use notación científica.

Las cantidades, páginas, medidas, peso y número de colección deben ser enteros
no negativos. El precio se guarda como número decimal. Al importar se aceptan
tanto coma como punto decimal; el valor se normaliza a dos decimales.

Los nombres de autoría y contribuciones deberían seguir el formato
`Apellidos, Nombre`. Varias personas se separan con punto y coma. Un nombre que
no sigue esa forma produce un aviso para revisión, pero no se reordena
automáticamente porque los apellidos compuestos, seudónimos y entidades no se
pueden interpretar con seguridad.

No se admiten fórmulas en las celdas importadas.

## Analizar e importar

1. Selecciona el Excel completado en la misma sección de **Datos**.
2. Pulsa **Analizar cambios del Excel**.
3. Revisa las fichas y los campos que cambiarán, incluidos los cambios
   automáticos en la descripción.
4. Corrige en el Excel cualquier error bloqueante y vuelve a analizarlo.
5. Decide si las fichas modificadas que estén en borrador deben consolidarse.
6. Marca la confirmación y pulsa **Aplicar cambios del Excel**.

Los errores de números, listas cerradas, referencias, cabeceras o estructura
impiden aplicar el archivo. Los avisos de nombres o ISBN se muestran por
separado y no bloquean.

La aplicación compara cada campo editado con el valor que tenía al exportarse.
Si ese mismo campo o el estado de la ficha cambió después en la base de datos,
la importación se detiene para no sobrescribir trabajo posterior. También
comprueba que el archivo aplicado sea exactamente el que se mostró en la vista
previa.

Todos los cambios se guardan en una sola transacción. Si falla una ficha, no se
modifica ninguna. Cada ficha conserva su estado salvo que se marque la opción
de consolidar tras la importación. Una ficha ya consolidada puede corregirse
mediante esta operación explícita y continúa consolidada.

La descripción se recalcula siempre a partir de los campos estructurados
importados. Si cambia el ISBN, también se normaliza la palabra clave `NOISBN`
con las mismas reglas del formulario.
