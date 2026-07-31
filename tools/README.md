# Lanzadores

Esta carpeta contiene lanzadores de doble clic para Windows y Ubuntu/Linux.

## Windows

Usa los ficheros `.bat`:

- `set-up-app.bat`
- `launch-app.bat`
- `stop-app.bat`
- `update-app.bat`
- `update-and-launch-app.bat`

Buscan GNU Make en el `PATH` con alguno de estos nombres:

- `make`
- `mingw32-make`
- `gmake`

## Ubuntu / Linux

Usa los ficheros `.desktop`:

- `set-up-app.desktop`
- `launch-app.desktop`
- `stop-app.desktop`
- `update-app.desktop`
- `update-and-launch-app.desktop`

Abren una terminal y ejecutan el script de shell correspondiente.

Según el entorno de escritorio, el primer uso puede requerir:

1. Marcar el fichero `.desktop` como ejecutable.
2. Elegir `Permitir ejecución` en el gestor de archivos.

Los scripts `.sh` también siguen disponibles para usarlos desde la terminal.

## Objetivos de actualización

- `make update-repo`: ejecuta exactamente `git pull origin main`.
- `make update`: ejecuta `make update-repo` y reinstala las dependencias del proyecto.

Los lanzadores llamados `update-app.*` usan `make update`.
Los lanzadores `update-and-launch-app.*` ejecutan `make update` y, si termina bien, `make dev`.
