# Configuración local

## Primer arranque

La instalación no requiere preparar `.env` manualmente. Al ejecutar
`tools/set-up-app.*`, `tools/launch-app.*` o el comando equivalente, `appctl`
copia `.env.example` a `.env` si todavía no existe. Una configuración ya
existente nunca se sobrescribe.

Los valores iniciales permiten abrir la aplicación sin claves externas. OCR y
catalogación usan Ollama; OpenAI e ISBNdb se habilitan al añadir sus claves.
Antes de trabajar con esos flujos debe estar iniciado Ollama y deben estar
instalados los modelos configurados.

## Grupos de variables

- Aplicación: `PROJECT_ROOT`, `APP_CHANNEL` y `APP_ICON_URL`.
- Actualización: `GIT_REMOTE`, `GIT_BRANCH`, timeouts y retención de backups.
- Servicios: `BACK_HOST`, `BACK_PORT`, `FRONT_HOST`, `FRONT_PORT` y `API_URL`.
- Datos: rutas de DuckDB, entradas, portadas, exportaciones y OCR.
- Snapshots: `BBDD_DIR`, estado, identidad del equipo y retención.
- Proveedores: modelos y selección de Ollama/OpenAI.
- Integraciones opcionales: `OPENAI_API_KEY` e `ISBNDB_API_KEY`.

`API_URL` puede quedar vacío: `appctl` lo deriva de `BACK_PORT`. `SYNC_ACTOR` y
`SYNC_DEVICE` también pueden quedar vacíos para usar la identidad del sistema.
Las rutas relativas se resuelven desde la raíz del repositorio.

## Validación

`setup`, `launch`, `dev`, `update` y `smoke` rechazan antes de operar:

- líneas o comillas inválidas en `.env`;
- puertos fuera de rango, iguales o incoherentes con `API_URL`;
- números, booleanos o proveedores no válidos;
- un `PROJECT_ROOT` que no corresponda al repositorio.

Las claves desconocidas, los ejemplos de identidad sin personalizar y un
proveedor OpenAI sin clave se muestran como avisos. Nunca se imprimen valores de
secretos.

## Diagnóstico

```bash
python3 scripts/appctl.py doctor
```

El diagnóstico no modifica la base ni aplica migraciones. Comprueba el remoto y
la rama Git, sincronización del entorno con el lock, integridad de recursos,
permisos de las rutas, esquema de DuckDB, disponibilidad de puertos, procesos y
conectividad con Ollama. Los errores incluyen la acción de recuperación.

Para verificar un arranque completo sin usar la base real:

```bash
python3 scripts/appctl.py smoke
```

El smoke crea una base y directorios temporales, arranca FastAPI y Streamlit,
consulta sus endpoints de salud y elimina después todos los datos temporales.
