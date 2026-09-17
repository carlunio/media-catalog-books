# Dependencias y empaquetado

## Contrato

`pyproject.toml` es la declaración mantenida a mano. Define rangos compatibles
para dependencias directas, herramientas de desarrollo y backend de build.
`requirements.lock` fija el entorno completo con hashes y es el fichero que se
instala tanto en equipos de usuario como en CI.

El lock incluye las herramientas de desarrollo porque este repositorio se
distribuye como aplicación autocontenida, no como una biblioteca. También fija
`colorama` y `tzdata`: son dependencias puras de Python que algunos paquetes
solicitan sólo en Windows y deben estar presentes en un lock generado en Linux.

## Instalación

`scripts/appctl.py setup` crea `.venv`, instala el lock con verificación de
hashes, instala el proyecto editable sin volver a resolver dependencias y
ejecuta `pip check`. Guarda junto al entorno las huellas de `pyproject.toml`,
`requirements.lock` y la versión menor de Python.

Al arrancar después de una actualización:

- si sólo cambia el metadato de proyecto, reinstala el proyecto editable;
- si cambia el lock o la versión menor de Python, reconstruye `.venv`;
- si falta una dependencia o la API `iso639.Language`, repara la instalación.

La reconstrucción completa evita conservar paquetes retirados y elimina
colisiones de módulos como la causada por instalar `iso639` en lugar de la
distribución correcta, `python-iso639`.

## Mantenimiento

Después de modificar dependencias en `pyproject.toml`:

```bash
make lock
make check-lock
make test
make build
```

Estos targets de lock usan el entorno de desarrollo ya creado con `make setup`.
No ejecutan `ensure-env` porque, durante la edición, `pyproject.toml` puede estar
deliberadamente adelantado respecto al lock que se está generando. El siguiente
`make test` o `make build` sí pasa por `appctl` y reconstruye el entorno.

`make lock` conserva las versiones ya fijadas siempre que sigan siendo
compatibles. Para una actualización deliberada de todas las dependencias:

```bash
make upgrade-lock
make check-lock
make test
make build
```

El lock se genera con `pip-tools`, hashes, finales de línea LF y sin rutas ni
índices locales. No debe editarse a mano.

## Validación

CI instala exclusivamente `requirements.lock`, comprueba que puede regenerarse
sin diferencias, ejecuta lint y tests, y construye rueda y paquete fuente. La
matriz usa Python 3.12 en Ubuntu y Windows para cubrir los dos sistemas de los
lanzadores de `tools/`.

La instalación de usuario soportada es un checkout Git, porque `appctl`, los
lanzadores y los recursos locales forman parte del producto. La rueda y el
paquete fuente son controles de integridad del namespace Python; no se publican
como instalador autónomo de la aplicación.
