# Dependencias y empaquetado

## Contrato

`pyproject.toml` es la declaración mantenida a mano. Define rangos compatibles
para las dependencias directas, las herramientas de desarrollo y el sistema de
construcción. `requirements.lock` fija el entorno completo con sumas de
comprobación y es el archivo que se
instala tanto en equipos de usuario como en CI.

El archivo de bloqueo incluye las herramientas de desarrollo porque este
repositorio se
distribuye como aplicación autocontenida, no como una biblioteca. También fija
`colorama` y `tzdata`: son dependencias puras de Python que algunos paquetes
solicitan solo en Windows y deben estar presentes en un archivo de bloqueo
creado en Linux.

## Instalación

`scripts/appctl.py setup` crea `.venv`, instala el archivo de bloqueo
verificando sus sumas de comprobación, instala el
proyecto editable sin volver a resolver dependencias y
ejecuta `pip check`. Guarda junto al entorno las huellas de `pyproject.toml`,
`requirements.lock` y la versión menor de Python.

Al arrancar después de una actualización:

- si solo cambia el metadato de proyecto, reinstala el proyecto editable;
- si cambia el archivo de bloqueo o la versión menor de Python, reconstruye
  `.venv`;
- si falta una dependencia o la API `iso639.Language`, repara la instalación.

La reconstrucción completa evita conservar paquetes retirados y elimina
colisiones entre módulos, como la causada por instalar `iso639` en lugar de la
distribución correcta, `python-iso639`.

## Mantenimiento

Después de modificar dependencias en `pyproject.toml`:

```bash
make lock
make check-lock
make test
make build
```

Estos objetivos usan el entorno de desarrollo ya creado con `make setup`. No
ejecutan `ensure-env` porque, durante la edición, `pyproject.toml` puede estar
deliberadamente adelantado respecto al archivo de bloqueo que se está generando.
El siguiente
`make test` o `make build` sí pasa por `appctl` y reconstruye el entorno.

`make lock` conserva las versiones ya fijadas siempre que sigan siendo
compatibles. Para una actualización deliberada de todas las dependencias:

```bash
make upgrade-lock
make check-lock
make test
make build
```

El archivo de bloqueo se genera con `pip-tools`, sumas de comprobación, finales
de línea LF y sin rutas ni
índices locales. No debe editarse a mano.

## Validación

La integración continua instala exclusivamente `requirements.lock`, comprueba
que puede regenerarse sin diferencias, ejecuta el análisis estático y las
pruebas, y construye la rueda y el paquete fuente. La
matriz usa Python 3.12 en Ubuntu y Windows para cubrir los dos sistemas de los
lanzadores de `tools/`.

La instalación de usuario compatible es una copia de trabajo Git, porque
`appctl`, los
lanzadores y los recursos locales forman parte del producto. La rueda y el
paquete fuente son controles de integridad del espacio de nombres de Python; no
se publican
como instalador autónomo de la aplicación.
