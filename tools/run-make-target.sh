#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"

TARGET="${1:-}"
ACTION_LABEL="${2:-$TARGET}"

if [[ -z "$TARGET" ]]; then
    echo "Uso: $(basename "$0") <make-target> [etiqueta]"
    echo
    read -r -p "Pulsa Intro para cerrar..."
    exit 1
fi

cd "$REPO_DIR" || exit 1

echo "${ACTION_LABEL}..."

if command -v make >/dev/null 2>&1; then
    make "$TARGET"
    EXIT_CODE=$?
else
    echo "Error: 'make' no está disponible en el PATH."
    echo "Instala GNU Make y vuelve a intentarlo."
    EXIT_CODE=127
fi

echo
if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "Proceso terminado correctamente."
else
    echo "El comando ha fallado con el código $EXIT_CODE."
fi

echo
read -r -p "Pulsa Intro para cerrar..."
exit "$EXIT_CODE"
