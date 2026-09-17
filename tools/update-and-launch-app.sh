#!/usr/bin/env bash

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
exec "$SCRIPT_DIR/run-appctl.sh" update-and-launch "Comprobando actualizaciones y arrancando la aplicación"
