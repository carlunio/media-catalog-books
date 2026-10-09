# Lanzadores

Esta carpeta contiene accesos de doble clic para Windows y Ubuntu/Linux. Los
ficheros `.bat`, `.sh` y `.desktop` no implementan otra forma de arrancar la
aplicación: todos delegan en `scripts/appctl.py`. GNU Make sigue siendo opcional
y se utiliza principalmente durante el desarrollo.

## Equivalencias

| Acción | Lanzador | `appctl` | Make |
| --- | --- | --- | --- |
| Preparar | `set-up-app.*` | `setup` | `make setup` |
| Arrancar | `launch-app.*` | `launch` | `make start` |
| Actualizar sin arrancar | `update-app.*` | `update` | `make update` |
| Detener | `stop-app.*` | `stop` | `make stop` |
| Diagnosticar | `doctor-app.*` | `doctor` | `make doctor` |

`launch` ya comprueba `origin/main`, actualiza cuando es seguro, prepara la
instalación y arranca backend y frontend. `update-and-launch-app.*` se conserva
como alias heredado para accesos directos antiguos, pero hace exactamente lo
mismo que `launch-app.*`.

`set-up-app.*` equivale a `make setup`. La reinstalación forzada de
`make install` no tiene lanzador de doble clic y se reserva para reparar o
recrear deliberadamente el entorno.

## Requisitos

- Python 3.12 o posterior.
- Git para descargar actualizaciones.
- Acceso a Internet durante la preparación inicial o al instalar dependencias
  nuevas.

## Windows

Usa los ficheros `.bat`. El lanzador común busca Python mediante `py -3.12`,
`py -3` y `python`, en ese orden.

## Ubuntu / Linux

Usa los ficheros `.desktop`. Estos abren una terminal y ejecutan el script `.sh`
correspondiente. El primer uso puede requerir marcar el archivo `.desktop` como
ejecutable y elegir **Permitir ejecución**.

## Controlador

`appctl` centraliza la operación:

- `setup`: crea `.env` si falta, prepara recursos, crea `.venv` e instala las
  dependencias declaradas en `pyproject.toml`;
- `launch`: comprueba `origin/main`, aplica una actualización segura y arranca
  backend y frontend sin recarga;
- `dev`: arranca con recarga y no consulta ni modifica Git;
- `update`: comprueba y aplica la actualización estable sin arrancar;
- `stop`: detiene únicamente los procesos registrados por `appctl`;
- `doctor`: comprueba configuración, Python, Git, dependencias, recursos, rutas,
  DuckDB, puertos, proveedores y procesos;
- `update-and-launch`: alias heredado de `launch`.

Puede ejecutarse directamente:

```bash
python3 scripts/appctl.py doctor
```

Los procesos activos se identifican mediante `.runtime/appctl.json`, que no se
versiona. Si no hay conexión, `launch` usa la versión instalada. Si la rama no es
`main`, hay cambios locales o el historial no permite un avance directo
(`fast-forward`), omite la actualización sin sobrescribir nada. Antes de aplicar
una versión nueva crea una copia de seguridad de DuckDB y revierte el código y
los datos si la preparación o el arranque fallan.
