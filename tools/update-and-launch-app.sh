#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"

cd "$REPO_DIR" || exit 1

if ! command -v make >/dev/null 2>&1; then
    echo "Error: make no está disponible en el PATH."
    echo "Instala GNU Make y vuelve a intentarlo."
    echo
    read -r -p "Pulsa Intro para cerrar..."
    exit 127
fi

echo "Actualizando la aplicación..."
make update
UPDATE_EXIT=$?
if [[ "$UPDATE_EXIT" -ne 0 ]]; then
    echo
    echo "La actualización ha fallado con el código $UPDATE_EXIT."
    echo "No se arrancará la aplicación."
    echo
    read -r -p "Pulsa Intro para cerrar..."
    exit "$UPDATE_EXIT"
fi

echo
echo "Arrancando la aplicación..."
make dev
EXIT_CODE=$?

echo
if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "Proceso terminado correctamente."
else
    echo "El arranque ha terminado con el código $EXIT_CODE."
fi

echo
read -r -p "Pulsa Intro para cerrar..."
exit "$EXIT_CODE"
