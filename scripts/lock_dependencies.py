from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
LOCK_PATH = PROJECT_ROOT / "requirements.lock"
COMPILE_COMMAND = "python3 scripts/lock_dependencies.py"


def _compile(output_path: Path, *, upgrade: bool) -> None:
    command = [
        sys.executable,
        "-m",
        "piptools",
        "compile",
        "--quiet",
        "--extra",
        "dev",
        "--all-build-deps",
        "--generate-hashes",
        "--allow-unsafe",
        "--strip-extras",
        "--no-annotate",
        "--resolver",
        "backtracking",
        "--newline",
        "lf",
        "--no-emit-index-url",
        "--no-emit-options",
        "--output-file",
        str(output_path),
    ]
    if upgrade:
        command.append("--upgrade")
    command.append(PYPROJECT_PATH.name)

    env = os.environ.copy()
    env["CUSTOM_COMPILE_COMMAND"] = COMPILE_COMMAND
    subprocess.run(command, cwd=PROJECT_ROOT, env=env, check=True)


def _check_lock() -> int:
    if not LOCK_PATH.exists():
        print(f"Falta el lock de dependencias: {LOCK_PATH}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="books-lock-") as tmp_dir:
        candidate_path = Path(tmp_dir) / LOCK_PATH.name
        shutil.copy2(LOCK_PATH, candidate_path)
        _compile(candidate_path, upgrade=False)
        if candidate_path.read_bytes() != LOCK_PATH.read_bytes():
            print(
                "requirements.lock no coincide con pyproject.toml; "
                "ejecuta `make lock`.",
                file=sys.stderr,
            )
            return 1

    print("requirements.lock esta sincronizado con pyproject.toml.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Genera o valida requirements.lock desde pyproject.toml."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Comprueba el lock sin modificar el repositorio.",
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        help="Actualiza todas las dependencias a las versiones compatibles mas recientes.",
    )
    args = parser.parse_args()

    if args.check:
        return _check_lock()
    _compile(LOCK_PATH, upgrade=args.upgrade)
    print(f"Lock actualizado: {LOCK_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
