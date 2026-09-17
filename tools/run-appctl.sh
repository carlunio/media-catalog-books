#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
COMMAND="${1:-}"
ACTION_LABEL="${2:-$COMMAND}"

if [[ -z "$COMMAND" ]]; then
    echo "Uso: $(basename "$0") <comando-appctl> [etiqueta]"
    echo
    read -r -p "Pulsa Intro para cerrar..."
    exit 1
fi

PYTHON_CMD=""
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 \
        && "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' >/dev/null 2>&1; then
        PYTHON_CMD="$candidate"
        break
    fi
done

if [[ -z "$PYTHON_CMD" ]]; then
    echo "Error: se requiere Python 3.12 o posterior en el PATH."
    echo
    read -r -p "Pulsa Intro para cerrar..."
    exit 127
fi

cd "$REPO_DIR" || exit 1
echo "${ACTION_LABEL}..."
"$PYTHON_CMD" "$REPO_DIR/scripts/appctl.py" "$COMMAND"
EXIT_CODE=$?

echo
if [[ "$EXIT_CODE" -eq 0 ]]; then
    echo "Proceso terminado correctamente."
else
    echo "El comando ha fallado con el código $EXIT_CODE."
fi

echo
read -r -p "Pulsa Intro para cerrar..."
exit "$EXIT_CODE"
