# Dependencias y empaquetado

## Contrato

`pyproject.toml` es la única declaración de dependencias del proyecto. Sus
secciones tienen responsabilidades distintas:

- `project.dependencies` contiene las dependencias de ejecución;
- `project.optional-dependencies.dev` contiene las herramientas de desarrollo,
  pruebas y construcción;
- `build-system.requires` contiene lo necesario para construir el paquete.

Las dependencias directas usan intervalos compatibles, por ejemplo
`openpyxl>=3.1,<4`. `pip` resuelve en cada instalación las versiones concretas y
sus dependencias indirectas dentro de esos límites. El repositorio no mantiene
un archivo de bloqueo separado.

`colorama` y `tzdata` figuran de forma explícita porque la aplicación debe
funcionar tanto en Linux como en Windows.

## Instalación

```bash
python3 scripts/appctl.py setup
```

`setup` prepara los recursos, crea `.venv` cuando hace falta e instala el
proyecto editable con sus herramientas de desarrollo mediante el equivalente a:

```bash
python -m pip install --upgrade --editable ".[dev]"
python -m pip check
```

El controlador guarda dentro de `.venv` la versión menor de Python, la huella
completa de `pyproject.toml` y una huella específica de sus secciones de
dependencias.

Al arrancar después de una actualización:

- si cambia la versión menor de Python o la declaración de dependencias,
  reconstruye `.venv` para no conservar paquetes retirados;
- si solo cambia otro metadato de `pyproject.toml`, actualiza la instalación
  editable sin volver a resolver las dependencias;
- si falta un módulo necesario, repara la instalación;
- si no cambia nada, conserva el entorno existente y arranca directamente.

Una instalación anterior que todavía tenga el antiguo estado basado en
`requirements.lock` se reconstruye una sola vez al ejecutar `setup`.

### Primera actualización desde el sistema anterior

El `appctl` anterior intentaba usar `requirements.lock` después de actualizar el
código. Como ese archivo ya no existe, una copia instalada con ese controlador
necesita una actualización manual única:

```bash
python3 scripts/appctl.py stop
git pull --ff-only origin main
python3 scripts/appctl.py setup
```

A partir de ahí, el controlador nuevo vuelve a gestionar las actualizaciones
automáticas con `pyproject.toml`.

## Mantenimiento

Después de añadir, eliminar o cambiar una dependencia en `pyproject.toml`:

```bash
make setup
make lint
make test
make build
```

`make install` fuerza una reconstrucción completa del entorno y una resolución
nueva de todas las dependencias compatibles:

```bash
make install
```

También puede hacerse sin GNU Make:

```bash
python3 scripts/appctl.py setup --force
```

La construcción usa el aislamiento estándar de `python -m build`, que instala
en un entorno temporal los requisitos declarados en `build-system.requires`.

## Reproducibilidad

Dos instalaciones realizadas en fechas distintas pueden resolver versiones
menores diferentes dentro de los intervalos declarados. A cambio, la instalación
no depende de un lock generado en otro sistema operativo ni de hashes de ruedas
específicas de una plataforma.

La integración continua instala directamente el extra `dev` desde
`pyproject.toml` en Python 3.12 sobre Ubuntu y Windows. Después ejecuta lint,
pruebas, smoke test y construcción del paquete. Estos controles detectan si una
versión nueva compatible introduce una incompatibilidad.

La generación y lectura de los libros de revisión `.xlsx` usa `openpyxl`. El
formato OOXML conserva Unicode y no comparte la codificación `windows-1252` del
TXT específico para AbeBooks.
