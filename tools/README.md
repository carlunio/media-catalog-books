# Lanzadores

Esta carpeta contiene lanzadores de doble clic para Windows y Ubuntu/Linux.
Todos delegan en `scripts/appctl.py`; GNU Make no es un requisito de usuario.

## Requisitos

- Python 3.12 o posterior.
- Git para descargar actualizaciones.
- Acceso a Internet durante la preparación inicial.

## Windows

Usa los ficheros `.bat`:

- `set-up-app.bat`
- `launch-app.bat`
- `stop-app.bat`
- `update-app.bat`
- `update-and-launch-app.bat`
- `doctor-app.bat`

El lanzador busca Python mediante `py -3.12`, `py -3` y `python`, en ese
orden.

## Ubuntu / Linux

Usa los ficheros `.desktop` equivalentes. Abren una terminal y ejecutan el
script `.sh` correspondiente. El primer uso puede requerir marcar el fichero
`.desktop` como ejecutable y elegir `Permitir ejecución`.

## Controlador

`appctl` centraliza la operación:

- `setup`: crea `.env` si falta, prepara recursos, crea `.venv` e instala dependencias.
- `launch`: comprueba `origin/main`, aplica una actualización segura y
  arranca backend y frontend sin recarga.
- `dev`: arranca con recarga y no consulta ni modifica Git.
- `update`: fuerza la comprobación estable sin arrancar la aplicación.
- `stop`: detiene únicamente los procesos registrados por `appctl`.
- `doctor`: comprueba configuración, Python, Git, lock, recursos, rutas, DuckDB,
  puertos, proveedores y procesos.
- `update-and-launch`: alias compatible del nuevo `launch` automático.

Puede ejecutarse directamente:

```bash
python3 scripts/appctl.py doctor
```

Los procesos activos se identifican mediante `.runtime/appctl.json`, que no
se versiona. Los targets Make siguen disponibles como accesos para desarrollo.

Si no hay conexión, `launch` usa la versión instalada. Si la rama no es
`main`, hay cambios locales o el historial no permite fast-forward, omite la
actualización sin sobrescribir nada. Antes de aplicar una versión nueva crea un
backup de DuckDB y revierte código y datos si la preparación o el arranque
fallan.
