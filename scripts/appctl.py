from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
VENV_DIR = PROJECT_ROOT / ".venv"
RUNTIME_DIR = PROJECT_ROOT / ".runtime"
RUNTIME_STATE_PATH = RUNTIME_DIR / "appctl.json"
OPERATION_LOCK_PATH = RUNTIME_DIR / "operation.lock"
UPDATE_STATUS_PATH = RUNTIME_DIR / "last-update.json"
ENV_PATH = PROJECT_ROOT / ".env"
ENV_EXAMPLE_PATH = PROJECT_ROOT / ".env.example"
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
LOCK_PATH = PROJECT_ROOT / "requirements.lock"
PREPARE_ASSETS_SCRIPT = PROJECT_ROOT / "scripts" / "prepare_local_assets.py"
INIT_DB_SCRIPT = PROJECT_ROOT / "scripts" / "init_db.py"
FRONTEND_APP = PROJECT_ROOT / "src" / "frontend" / "app.py"
BACKEND_APP = "src.backend.main:app"
MIN_PYTHON = (3, 12)
REQUIRED_MODULES = (
    "uvicorn",
    "streamlit",
    "fastapi",
    "duckdb",
    "openai",
    "langgraph",
    "langcodes",
    "iso639",
    "pytest",
    "ruff",
    "black",
    "build",
    "piptools",
)
DOTENV_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
TRUE_VALUES = frozenset({"1", "true", "t", "yes", "y", "on"})
FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
LEGACY_ENV_KEYS = frozenset({"OCR_VISION_MODEL"})


class AppCtlError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConfigurationIssue:
    level: str
    message: str


@dataclass
class UpdateResult:
    status: str
    message: str
    old_revision: str | None = None
    new_revision: str | None = None
    old_version: str | None = None
    new_version: str | None = None
    database_path: str | None = None
    database_backup: str | None = None
    database_existed: bool = False
    dependencies_changed: bool = False

    @property
    def updated(self) -> bool:
        return self.status == "updated_pending"


def _venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def _read_dotenv(path: Path | None = None) -> dict[str, str]:
    path = path or ENV_PATH
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue

        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        value = value.strip()
        if not DOTENV_KEY_PATTERN.fullmatch(key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _ensure_env_file() -> bool:
    if ENV_PATH.exists():
        return False
    if not ENV_EXAMPLE_PATH.exists():
        raise AppCtlError(f"Falta la plantilla de configuración: {ENV_EXAMPLE_PATH}")

    temporary = ENV_PATH.with_name(f".{ENV_PATH.name}.creating")
    temporary.unlink(missing_ok=True)
    try:
        shutil.copyfile(ENV_EXAMPLE_PATH, temporary)
        temporary.replace(ENV_PATH)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Configuración local creada en {ENV_PATH}.")
    return True


def _dotenv_syntax_issues(path: Path) -> list[ConfigurationIssue]:
    if not path.exists():
        return []

    issues: list[ConfigurationIssue] = []
    seen: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        return [ConfigurationIssue("ERROR", f"No se puede leer {path.name}: {exc}")]

    for line_number, raw_line in enumerate(lines, start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            issues.append(
                ConfigurationIssue(
                    "ERROR",
                    f"{path.name}:{line_number}: falta '=' en la asignación.",
                )
            )
            continue

        key, value = line.split("=", maxsplit=1)
        key = key.strip()
        value = value.strip()
        if not DOTENV_KEY_PATTERN.fullmatch(key):
            issues.append(
                ConfigurationIssue(
                    "ERROR",
                    f"{path.name}:{line_number}: nombre de clave no válido: {key!r}.",
                )
            )
            continue
        if key in seen:
            issues.append(
                ConfigurationIssue(
                    "AVISO",
                    f"{path.name}:{line_number}: {key} ya estaba definida en la línea {seen[key]}.",
                )
            )
        seen[key] = line_number
        if value[:1] in {"'", '"'} and (len(value) < 2 or value[-1] != value[0]):
            issues.append(
                ConfigurationIssue(
                    "ERROR",
                    f"{path.name}:{line_number}: comillas sin cerrar en {key}.",
                )
            )
    return issues


def _configuration_issues(
    env: dict[str, str],
    *,
    env_path: Path | None = None,
    example_path: Path | None = None,
) -> list[ConfigurationIssue]:
    env_path = env_path or ENV_PATH
    example_path = example_path or ENV_EXAMPLE_PATH
    issues = _dotenv_syntax_issues(env_path)

    local_values = _read_dotenv(env_path)
    known_keys = set(_read_dotenv(example_path)) | set(LEGACY_ENV_KEYS)
    if known_keys:
        unknown = sorted(set(local_values) - known_keys)
        if unknown:
            issues.append(
                ConfigurationIssue(
                    "AVISO",
                    "Claves no reconocidas en .env: " + ", ".join(unknown) + ".",
                )
            )

    ports: dict[str, int] = {}
    for name, default in (("BACK_PORT", 8000), ("FRONT_PORT", 8501)):
        try:
            ports[name] = _port(env, name, default)
        except AppCtlError as exc:
            issues.append(ConfigurationIssue("ERROR", str(exc)))
    if ports.get("BACK_PORT") == ports.get("FRONT_PORT"):
        issues.append(
            ConfigurationIssue(
                "ERROR", "BACK_PORT y FRONT_PORT deben usar puertos diferentes."
            )
        )

    def validate_number(
        name: str,
        *,
        minimum: float,
        maximum: float | None = None,
        integer: bool = False,
        allow_blank: bool = False,
    ) -> None:
        raw = str(env.get(name, "") or "").strip()
        if not raw and allow_blank:
            return
        if not raw:
            return
        try:
            value = int(raw) if integer else float(raw)
        except ValueError:
            kind = "entero" if integer else "numérico"
            issues.append(ConfigurationIssue("ERROR", f"{name} debe ser {kind}."))
            return
        if value < minimum or (maximum is not None and value > maximum):
            if maximum is None:
                expected = f"mayor o igual que {minimum:g}"
            else:
                expected = f"entre {minimum:g} y {maximum:g}"
            issues.append(ConfigurationIssue("ERROR", f"{name} debe estar {expected}."))

    for name in (
        "APP_UPDATE_TIMEOUT_SECONDS",
        "APP_STARTUP_TIMEOUT_SECONDS",
        "REQUEST_TIMEOUT_SECONDS",
        "API_TIMEOUT_SECONDS",
        "API_LONG_TIMEOUT_SECONDS",
    ):
        validate_number(name, minimum=0.001)
    validate_number("OLLAMA_TIMEOUT_SECONDS", minimum=0.001, allow_blank=True)
    for name in (
        "GOOGLE_BOOKS_MIN_INTERVAL_SECONDS",
        "OPENLIBRARY_MIN_INTERVAL_SECONDS",
    ):
        validate_number(name, minimum=0)
    validate_number("CATALOG_ARBITER_MIN_CONFIDENCE", minimum=0, maximum=1)
    validate_number("APP_UPDATE_BACKUP_KEEP", minimum=1, integer=True)
    validate_number("SYNC_RETENTION_DAYS", minimum=0, integer=True)
    validate_number("SYNC_KEEP_MIN", minimum=1, integer=True)
    validate_number("WORKFLOW_MAX_ATTEMPTS", minimum=1, integer=True)

    for name in (
        "OCR_RESIZE_TO_1800_DEFAULT",
        "OCR_USE_SIDECAR",
        "CATALOG_ARBITER_ENABLED",
    ):
        raw = str(env.get(name, "") or "").strip().lower()
        if raw and raw not in TRUE_VALUES | FALSE_VALUES:
            issues.append(
                ConfigurationIssue(
                    "ERROR", f"{name} debe ser true o false; valor no reconocido."
                )
            )

    provider_contracts = {
        "OCR_PROVIDER": {"ollama", "openai"},
        "CATALOG_PROVIDER": {"auto", "ollama", "openai"},
        "CATALOG_ARBITER_PROVIDER": {"auto", "ollama", "openai"},
    }
    providers: dict[str, str] = {}
    for name, allowed in provider_contracts.items():
        value = str(env.get(name, "") or "").strip().lower()
        providers[name] = value
        if value and value not in allowed:
            issues.append(
                ConfigurationIssue(
                    "ERROR",
                    f"{name} debe ser uno de: {', '.join(sorted(allowed))}.",
                )
            )

    if (
        "openai" in providers.values()
        and not str(env.get("OPENAI_API_KEY", "") or "").strip()
    ):
        issues.append(
            ConfigurationIssue(
                "AVISO",
                "Hay un proveedor OpenAI activo, pero OPENAI_API_KEY está vacía.",
            )
        )

    api_url = str(env.get("API_URL", "") or "").strip()
    try:
        parsed_api_url = urlsplit(api_url)
        api_port = parsed_api_url.port
    except ValueError:
        parsed_api_url = None
        api_port = None
    if (
        parsed_api_url is None
        or parsed_api_url.scheme not in {"http", "https"}
        or not parsed_api_url.hostname
    ):
        issues.append(
            ConfigurationIssue("ERROR", "API_URL debe ser una URL HTTP válida.")
        )
    elif parsed_api_url.hostname in {"127.0.0.1", "localhost", "::1"}:
        expected_port = ports.get("BACK_PORT")
        effective_port = api_port or (443 if parsed_api_url.scheme == "https" else 80)
        if expected_port is not None and effective_port != expected_port:
            issues.append(
                ConfigurationIssue(
                    "ERROR",
                    "API_URL debe usar el mismo puerto local que BACK_PORT.",
                )
            )

    configured_root = Path(str(env.get("PROJECT_ROOT", PROJECT_ROOT))).resolve()
    if configured_root != PROJECT_ROOT.resolve():
        issues.append(
            ConfigurationIssue(
                "ERROR",
                f"PROJECT_ROOT debe resolver a la raíz del repositorio: {PROJECT_ROOT}.",
            )
        )

    for name in ("SYNC_ACTOR", "SYNC_DEVICE"):
        value = str(env.get(name, "") or "").strip().lower()
        if value in {"tu-nombre", "tu-equipo"}:
            issues.append(
                ConfigurationIssue(
                    "AVISO",
                    f"{name} conserva el ejemplo; déjalo vacío o personalízalo.",
                )
            )
    return issues


def _app_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key, value in _read_dotenv().items():
        env.setdefault(key, value)
    configured_root = Path(str(env.get("PROJECT_ROOT", PROJECT_ROOT))).expanduser()
    if not configured_root.is_absolute():
        configured_root = PROJECT_ROOT / configured_root
    env["PROJECT_ROOT"] = str(configured_root.resolve())
    if not str(env.get("API_URL", "") or "").strip():
        back_port = str(env.get("BACK_PORT", "8000") or "8000").strip()
        env["API_URL"] = f"http://127.0.0.1:{back_port}"
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    return env


def _validated_environment(*, create_env: bool) -> dict[str, str]:
    if create_env:
        _ensure_env_file()
    env = _app_environment()
    issues = _configuration_issues(env)
    errors = [issue.message for issue in issues if issue.level == "ERROR"]
    if errors:
        details = "\n".join(f"- {message}" for message in errors)
        raise AppCtlError(f"Configuración inválida:\n{details}")
    for issue in issues:
        if issue.level == "AVISO":
            print(f"Aviso de configuración: {issue.message}")
    return env


def _run(
    command: Sequence[str | Path],
    *,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    printable = [str(part) for part in command]
    try:
        return subprocess.run(
            printable,
            cwd=PROJECT_ROOT,
            env=env,
            check=check,
            text=True,
            capture_output=capture_output,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise AppCtlError(f"No se ha encontrado el comando: {printable[0]}") from exc
    except subprocess.CalledProcessError as exc:
        detail = str(exc.stderr or exc.stdout or "").strip()
        suffix = f": {detail}" if detail else ""
        raise AppCtlError(
            f"El comando ha fallado con código {exc.returncode}: "
            f"{' '.join(printable)}{suffix}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise AppCtlError(
            f"El comando superó el tiempo máximo de espera: {' '.join(printable)}"
        ) from exc


def _require_python_version() -> None:
    if sys.version_info < MIN_PYTHON:
        required = ".".join(str(part) for part in MIN_PYTHON)
        current = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        raise AppCtlError(
            f"Se requiere Python {required} o posterior; versión activa: {current}."
        )


def _prepare_assets(env: dict[str, str]) -> None:
    _run([sys.executable, PREPARE_ASSETS_SCRIPT], env=env)


def _create_venv(env: dict[str, str]) -> None:
    if _venv_python().exists():
        return
    print(f"Creando entorno virtual en {VENV_DIR}...")
    _run([sys.executable, "-m", "venv", VENV_DIR], env=env)


def _dependency_state_path() -> Path:
    return VENV_DIR / ".media-catalog-dependencies.json"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _desired_dependency_state() -> dict[str, str]:
    missing = [path for path in (PYPROJECT_PATH, LOCK_PATH) if not path.exists()]
    if missing:
        raise AppCtlError(
            "Faltan ficheros de instalacion: "
            + ", ".join(str(path) for path in missing)
        )
    return {
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "pyproject_sha256": _file_sha256(PYPROJECT_PATH),
        "lock_sha256": _file_sha256(LOCK_PATH),
    }


def _read_dependency_state() -> dict[str, str]:
    try:
        data = json.loads(_dependency_state_path().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(key): str(value) for key, value in data.items()}


def _write_dependency_state(state: dict[str, str]) -> None:
    path = _dependency_state_path()
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _install_project(env: dict[str, str]) -> None:
    python = _venv_python()
    _run(
        [
            python,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--no-build-isolation",
            "--editable",
            PROJECT_ROOT,
        ],
        env=env,
    )
    _run([python, "-m", "pip", "check"], env=env)


def _install_dependencies(env: dict[str, str]) -> None:
    python = _venv_python()
    _run(
        [
            python,
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "--requirement",
            LOCK_PATH,
        ],
        env=env,
    )
    _install_project(env)
    _write_dependency_state(_desired_dependency_state())


def _remove_managed_venv() -> None:
    if not VENV_DIR.exists():
        return
    if VENV_DIR.is_symlink():
        raise AppCtlError(
            f"No se reconstruira un entorno virtual que sea enlace simbolico: {VENV_DIR}"
        )
    shutil.rmtree(VENV_DIR)


def _missing_modules(env: dict[str, str], *, python: Path | None = None) -> list[str]:
    python = python or _venv_python()
    if not python.exists():
        return list(REQUIRED_MODULES)

    code = (
        "import importlib, importlib.util, json; "
        f"mods={REQUIRED_MODULES!r}; "
        "missing=[m for m in mods if importlib.util.find_spec(m) is None]; "
        "iso=importlib.import_module('iso639') if 'iso639' not in missing else None; "
        "missing.extend(['python-iso639 (iso639.Language)'] "
        "if iso is not None and not hasattr(iso, 'Language') else []); "
        "print(json.dumps(missing))"
    )
    result = _run([python, "-c", code], env=env, capture_output=True)
    try:
        return [str(item) for item in json.loads(result.stdout)]
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AppCtlError("No se pudo comprobar el entorno virtual.") from exc


def _ensure_environment(env: dict[str, str], *, force_install: bool = False) -> None:
    _prepare_assets(env)
    desired_state = _desired_dependency_state()
    created = not _venv_python().exists()
    installed_state = _read_dependency_state() if not created else {}
    lock_changed = any(
        installed_state.get(key) != desired_state[key]
        for key in ("python", "lock_sha256")
    )
    if not created and (force_install or lock_changed):
        print("Reconstruyendo el entorno para aplicar el lock de dependencias...")
        _remove_managed_venv()
        created = True
    _create_venv(env)
    missing = _missing_modules(env)
    if created or missing:
        if missing:
            print(f"Instalando dependencias que faltan: {', '.join(missing)}")
        _install_dependencies(env)
        return

    if installed_state.get("pyproject_sha256") != desired_state["pyproject_sha256"]:
        print("Actualizando los metadatos del proyecto instalado...")
        _install_project(env)
        _write_dependency_state(desired_state)


def _as_positive_float(env: dict[str, str], name: str, default: float) -> float:
    raw = str(env.get(name, default)).strip()
    try:
        value = float(raw)
    except ValueError as exc:
        raise AppCtlError(
            f"{name} debe ser numérico; valor recibido: {raw!r}."
        ) from exc
    if value <= 0:
        raise AppCtlError(f"{name} debe ser mayor que cero; valor recibido: {value}.")
    return value


def _as_positive_int(env: dict[str, str], name: str, default: int) -> int:
    raw = str(env.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise AppCtlError(f"{name} debe ser entero; valor recibido: {raw!r}.") from exc
    if value < 1:
        raise AppCtlError(f"{name} debe ser mayor que cero; valor recibido: {value}.")
    return value


@contextmanager
def _operation_lock() -> Iterator[None]:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    current_pid = os.getpid()

    for _attempt in range(2):
        try:
            descriptor = os.open(
                OPERATION_LOCK_PATH,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            try:
                owner_pid = int(OPERATION_LOCK_PATH.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                owner_pid = 0
            if owner_pid and _pid_command(owner_pid):
                raise AppCtlError(
                    f"Ya hay otra operación de appctl en curso con PID {owner_pid}."
                )
            try:
                OPERATION_LOCK_PATH.unlink()
            except FileNotFoundError:
                pass
            continue

        try:
            os.write(descriptor, f"{current_pid}\n".encode())
        finally:
            os.close(descriptor)
        break
    else:
        raise AppCtlError("No se pudo obtener el bloqueo de operación de appctl.")

    try:
        yield
    finally:
        try:
            owner_pid = int(OPERATION_LOCK_PATH.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            owner_pid = 0
        if owner_pid == current_pid:
            try:
                OPERATION_LOCK_PATH.unlink()
            except FileNotFoundError:
                pass


def _write_update_status(result: UpdateResult) -> None:
    try:
        RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        payload = {**asdict(result), "checked_at": datetime.now(UTC).isoformat()}
        tmp_path = UPDATE_STATUS_PATH.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(UPDATE_STATUS_PATH)
    except OSError as exc:
        print(
            f"Aviso: no se pudo guardar el estado de actualización: {exc}",
            file=sys.stderr,
        )


def _project_release_data(path: Path = PYPROJECT_PATH) -> tuple[str, str]:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return "0.0.0", ""

    project = document.get("project", {})
    lock_path = path.with_name(LOCK_PATH.name)
    dependency_data = {
        "build-system": document.get("build-system", {}),
        "requires-python": project.get("requires-python"),
        "dependencies": project.get("dependencies", []),
        "optional-dependencies": project.get("optional-dependencies", {}),
        "lock-sha256": _file_sha256(lock_path) if lock_path.exists() else None,
    }
    serialized = json.dumps(dependency_data, sort_keys=True, ensure_ascii=True)
    fingerprint = hashlib.sha256(serialized.encode()).hexdigest()
    return str(project.get("version") or "0.0.0"), fingerprint


def _git_output(
    arguments: Sequence[str],
    *,
    env: dict[str, str],
    timeout: float | None = None,
) -> str:
    result = _run(
        ["git", *arguments],
        env=env,
        capture_output=True,
        timeout=timeout,
    )
    return result.stdout.strip()


def _git_is_ancestor(
    ancestor: str,
    descendant: str,
    *,
    env: dict[str, str],
) -> bool:
    result = _run(
        ["git", "merge-base", "--is-ancestor", ancestor, descendant],
        env=env,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _skip_or_raise(
    *,
    automatic: bool,
    status: str,
    message: str,
) -> UpdateResult:
    if automatic:
        return UpdateResult(status=status, message=message)
    raise AppCtlError(message)


def _resolve_app_path(env: dict[str, str], name: str, default: str) -> Path:
    project_root = (
        Path(env.get("PROJECT_ROOT", str(PROJECT_ROOT))).expanduser().resolve()
    )
    path = Path(str(env.get(name, default) or default)).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def _checkpoint_database(path: Path, env: dict[str, str]) -> None:
    if not path.exists() or not _venv_python().exists():
        return
    code = (
        "import duckdb, sys; "
        "con=duckdb.connect(sys.argv[1]); "
        "con.execute('CHECKPOINT'); "
        "con.close()"
    )
    _run([_venv_python(), "-c", code, path], env=env, capture_output=True)


def _backup_database(
    env: dict[str, str], old_revision: str
) -> tuple[Path, Path | None, bool]:
    database_path = _resolve_app_path(env, "DB_PATH", "data/books.duckdb")
    if not database_path.exists():
        return database_path, None, False

    _checkpoint_database(database_path, env)
    backup_dir = _resolve_app_path(
        env, "APP_UPDATE_BACKUP_DIR", "data/backups/pre_update"
    )
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = backup_dir / (
        f"books_before_update_{timestamp}_{old_revision[:12]}.duckdb"
    )
    temporary_path = backup_path.with_suffix(".tmp")
    shutil.copy2(database_path, temporary_path)
    temporary_path.replace(backup_path)
    return database_path, backup_path, True


def _restore_database(result: UpdateResult) -> None:
    if not result.database_path:
        return
    database_path = Path(result.database_path)
    backup_path = Path(result.database_backup) if result.database_backup else None

    if backup_path and backup_path.exists():
        database_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = database_path.with_suffix(database_path.suffix + ".rollback")
        shutil.copy2(backup_path, temporary_path)
        temporary_path.replace(database_path)
    elif not result.database_existed and database_path.exists():
        database_path.unlink()


def _cleanup_update_backups(env: dict[str, str]) -> None:
    backup_dir = _resolve_app_path(
        env, "APP_UPDATE_BACKUP_DIR", "data/backups/pre_update"
    )
    if not backup_dir.exists():
        return
    keep = _as_positive_int(env, "APP_UPDATE_BACKUP_KEEP", 5)
    try:
        backups = sorted(
            backup_dir.glob("books_before_update_*.duckdb"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    except OSError as exc:
        print(f"Aviso: no se pudieron revisar los backups: {exc}", file=sys.stderr)
        return
    for path in backups[keep:]:
        try:
            path.unlink()
        except OSError as exc:
            print(f"Aviso: no se pudo eliminar {path.name}: {exc}", file=sys.stderr)


def _rollback_update(
    result: UpdateResult,
    env: dict[str, str],
    *,
    reason: str,
) -> UpdateResult:
    if not result.old_revision:
        raise AppCtlError(
            "No se conoce la revisión anterior para revertir la actualización."
        )

    _run(
        ["git", "reset", "--hard", result.old_revision],
        env=env,
        capture_output=True,
    )
    _restore_database(result)
    rolled_back = UpdateResult(
        **{
            **asdict(result),
            "status": "rolled_back",
            "message": reason,
            "new_revision": result.old_revision,
            "new_version": result.old_version,
        }
    )
    _write_update_status(rolled_back)
    return rolled_back


def _attempt_stable_update(
    env: dict[str, str],
    *,
    automatic: bool,
) -> UpdateResult:
    remote = str(env.get("GIT_REMOTE", "origin") or "origin").strip()
    branch = str(env.get("GIT_BRANCH", "main") or "main").strip()
    timeout = _as_positive_float(env, "APP_UPDATE_TIMEOUT_SECONDS", 20.0)

    if shutil.which("git") is None:
        return _skip_or_raise(
            automatic=automatic,
            status="unavailable",
            message="Git no está disponible; se mantiene la versión local.",
        )

    try:
        inside_worktree = _git_output(["rev-parse", "--is-inside-work-tree"], env=env)
        current_branch = _git_output(
            ["symbolic-ref", "--quiet", "--short", "HEAD"], env=env
        )
    except AppCtlError as exc:
        return _skip_or_raise(
            automatic=automatic,
            status="unavailable",
            message=f"No se pudo validar la instalación Git: {exc}",
        )

    if inside_worktree != "true":
        return _skip_or_raise(
            automatic=automatic,
            status="unavailable",
            message="La aplicación no está dentro de un repositorio Git.",
        )
    if current_branch != branch:
        return _skip_or_raise(
            automatic=automatic,
            status="skipped",
            message=(
                f"Actualización automática omitida: rama activa {current_branch!r}; "
                f"el canal estable es {branch!r}."
            ),
        )

    dirty = _git_output(["status", "--porcelain", "--untracked-files=all"], env=env)
    if dirty:
        return _skip_or_raise(
            automatic=automatic,
            status="skipped",
            message=(
                "Actualización automática omitida porque hay archivos versionados "
                "o no rastreados modificados."
            ),
        )

    old_revision = _git_output(["rev-parse", "HEAD"], env=env)
    old_version, old_dependencies = _project_release_data()
    try:
        _run(
            ["git", "fetch", "--quiet", remote, branch],
            env=env,
            capture_output=True,
            timeout=timeout,
        )
    except AppCtlError as exc:
        if not automatic:
            raise
        return UpdateResult(
            status="offline",
            message=f"No se pudo comprobar {remote}/{branch}; se mantiene la versión local: {exc}",
            old_revision=old_revision,
            new_revision=old_revision,
            old_version=old_version,
            new_version=old_version,
        )

    new_revision = _git_output(["rev-parse", "FETCH_HEAD"], env=env)
    if old_revision == new_revision:
        return UpdateResult(
            status="up_to_date",
            message=f"La versión local ya coincide con {remote}/{branch}.",
            old_revision=old_revision,
            new_revision=new_revision,
            old_version=old_version,
            new_version=old_version,
        )

    if not _git_is_ancestor(old_revision, new_revision, env=env):
        if _git_is_ancestor(new_revision, old_revision, env=env):
            message = (
                "La rama local contiene commits que no están publicados; "
                "no se modificará automáticamente."
            )
        else:
            message = (
                "La rama local y el canal estable han divergido; "
                "no se puede aplicar una actualización fast-forward."
            )
        return _skip_or_raise(
            automatic=automatic,
            status="skipped",
            message=message,
        )

    try:
        database_path, backup_path, database_existed = _backup_database(
            env, old_revision
        )
    except (AppCtlError, OSError) as exc:
        message = (
            "No se pudo crear el backup previo; no se aplicará la actualización "
            f"y se mantendrá la versión local: {exc}"
        )
        if not automatic:
            raise AppCtlError(message) from exc
        return UpdateResult(
            status="failed",
            message=message,
            old_revision=old_revision,
            new_revision=old_revision,
            old_version=old_version,
            new_version=old_version,
        )
    pending = UpdateResult(
        status="updated_pending",
        message=f"Aplicando actualización desde {remote}/{branch}.",
        old_revision=old_revision,
        new_revision=new_revision,
        old_version=old_version,
        database_path=str(database_path),
        database_backup=str(backup_path) if backup_path else None,
        database_existed=database_existed,
    )
    _write_update_status(pending)

    try:
        _run(
            ["git", "merge", "--ff-only", new_revision],
            env=env,
            capture_output=True,
        )
    except AppCtlError as exc:
        _run(
            ["git", "reset", "--hard", old_revision],
            env=env,
            capture_output=True,
            check=False,
        )
        _restore_database(pending)
        failed = UpdateResult(
            **{
                **asdict(pending),
                "status": "failed",
                "message": f"No se pudo aplicar la actualización: {exc}",
                "new_revision": old_revision,
                "new_version": old_version,
            }
        )
        _write_update_status(failed)
        if automatic:
            return failed
        raise AppCtlError(failed.message) from exc

    new_version, new_dependencies = _project_release_data()
    pending.new_version = new_version
    pending.dependencies_changed = old_dependencies != new_dependencies
    _write_update_status(pending)
    return pending


def _mark_update_success(result: UpdateResult, env: dict[str, str]) -> UpdateResult:
    if not result.updated:
        _write_update_status(result)
        return result
    completed = UpdateResult(
        **{
            **asdict(result),
            "status": "updated",
            "message": (
                f"Aplicación actualizada de {result.old_version} "
                f"a {result.new_version}."
            ),
        }
    )
    _cleanup_update_backups(env)
    _write_update_status(completed)
    return completed


def _port(env: dict[str, str], name: str, default: int) -> int:
    raw = str(env.get(name, default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise AppCtlError(
            f"{name} debe ser un puerto numérico; valor recibido: {raw!r}."
        ) from exc
    if not 1 <= value <= 65535:
        raise AppCtlError(
            f"{name} debe estar entre 1 y 65535; valor recibido: {value}."
        )
    return value


def _client_host(host: str) -> str:
    normalized = str(host or "").strip().strip("[]")
    if normalized in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return normalized


def _service_url(host: str, port: int, path: str = "") -> str:
    client_host = _client_host(host)
    authority = f"[{client_host}]" if ":" in client_host else client_host
    suffix = path if path.startswith("/") or not path else f"/{path}"
    return f"http://{authority}:{port}{suffix}"


def _port_is_available(host: str, port: int) -> bool:
    bind_host = str(host or "127.0.0.1").strip().strip("[]")
    if bind_host == "localhost":
        bind_host = "127.0.0.1"
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((bind_host, port))
    except OSError:
        return False
    return True


def _ensure_service_ports_available(env: dict[str, str]) -> None:
    services = (
        (
            "backend",
            str(env.get("BACK_HOST", "127.0.0.1") or "127.0.0.1").strip(),
            _port(env, "BACK_PORT", 8000),
        ),
        (
            "frontend",
            str(env.get("FRONT_HOST", "127.0.0.1") or "127.0.0.1").strip(),
            _port(env, "FRONT_PORT", 8501),
        ),
    )
    occupied = [
        f"{name} ({host}:{port})"
        for name, host, port in services
        if not _port_is_available(host, port)
    ]
    if occupied:
        raise AppCtlError(
            "No se puede arrancar porque el puerto ya está ocupado: "
            + ", ".join(occupied)
            + ". Cierra el proceso que lo usa o cambia el puerto en .env."
        )


def _service_commands(
    env: dict[str, str], *, reload_backend: bool, python: Path | None = None
) -> tuple[list[str], list[str]]:
    executable = str(python or _venv_python())
    back_host = str(env.get("BACK_HOST", "127.0.0.1") or "127.0.0.1").strip()
    front_host = str(env.get("FRONT_HOST", "127.0.0.1") or "127.0.0.1").strip()
    backend = [
        executable,
        "-m",
        "uvicorn",
        BACKEND_APP,
        "--host",
        back_host,
        "--port",
        str(_port(env, "BACK_PORT", 8000)),
    ]
    if reload_backend:
        backend.append("--reload")

    frontend = [
        executable,
        "-m",
        "streamlit",
        "run",
        str(FRONTEND_APP),
        "--server.address",
        front_host,
        "--server.port",
        str(_port(env, "FRONT_PORT", 8501)),
    ]
    return backend, frontend


@dataclass
class RunningServices:
    backend: subprocess.Popen[Any]
    frontend: subprocess.Popen[Any]
    controller_pid: int
    mode: str
    persist_runtime: bool = True


def _http_is_ready(url: str) -> bool:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=1.0) as response:
            return 200 <= int(response.status) < 300
    except (OSError, urllib.error.URLError):
        return False


def _stop_services(services: RunningServices | None) -> None:
    if services is None:
        return
    _stop_child(services.frontend)
    _stop_child(services.backend)
    if services.persist_runtime:
        _remove_runtime_state(controller_pid=services.controller_pid)


def _start_services(
    env: dict[str, str],
    *,
    reload_backend: bool,
    mode: str,
    persist_runtime: bool = True,
    python: Path | None = None,
) -> RunningServices:
    _ensure_service_ports_available(env)
    backend_command, frontend_command = _service_commands(
        env, reload_backend=reload_backend, python=python
    )
    back_host = str(env.get("BACK_HOST", "127.0.0.1") or "127.0.0.1").strip()
    front_host = str(env.get("FRONT_HOST", "127.0.0.1") or "127.0.0.1").strip()
    back_port = _port(env, "BACK_PORT", 8000)
    front_port = _port(env, "FRONT_PORT", 8501)
    controller_pid = os.getpid()

    print(f"Arrancando backend en {_service_url(back_host, back_port)}...")
    backend = _popen(backend_command, env)
    try:
        print(f"Arrancando frontend en {_service_url(front_host, front_port)}...")
        frontend = _popen(frontend_command, env)
    except BaseException:
        _stop_child(backend)
        raise

    services = RunningServices(
        backend=backend,
        frontend=frontend,
        controller_pid=controller_pid,
        mode=mode,
        persist_runtime=persist_runtime,
    )
    if persist_runtime:
        _write_runtime_state(
            {
                "version": 1,
                "mode": mode,
                "controller_pid": controller_pid,
                "backend_pid": backend.pid,
                "frontend_pid": frontend.pid,
                "started_at": datetime.now(UTC).isoformat(),
            }
        )

    timeout = _as_positive_float(env, "APP_STARTUP_TIMEOUT_SECONDS", 45.0)
    deadline = time.monotonic() + timeout
    backend_url = _service_url(back_host, back_port, "/health")
    frontend_url = _service_url(front_host, front_port, "/_stcore/health")
    try:
        while time.monotonic() < deadline:
            backend_exit = backend.poll()
            frontend_exit = frontend.poll()
            if backend_exit is not None:
                raise AppCtlError(
                    f"El backend terminó durante el arranque con código {backend_exit}."
                )
            if frontend_exit is not None:
                raise AppCtlError(
                    f"El frontend terminó durante el arranque con código {frontend_exit}."
                )
            if _http_is_ready(backend_url) and _http_is_ready(frontend_url):
                print("Aplicación preparada.")
                return services
            time.sleep(0.25)
    except BaseException:
        _stop_services(services)
        raise

    _stop_services(services)
    raise AppCtlError(
        f"La aplicación no respondió correctamente antes de {timeout:.0f} segundos."
    )


def _wait_for_services(services: RunningServices) -> int:
    while True:
        backend_exit = services.backend.poll()
        frontend_exit = services.frontend.poll()
        if backend_exit is not None:
            if backend_exit != 0:
                print(
                    f"El backend ha terminado con código {backend_exit}.",
                    file=sys.stderr,
                )
            return int(backend_exit)
        if frontend_exit is not None:
            if frontend_exit != 0:
                print(
                    f"El frontend ha terminado con código {frontend_exit}.",
                    file=sys.stderr,
                )
            return int(frontend_exit)
        time.sleep(0.5)


def _write_runtime_state(state: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = RUNTIME_STATE_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(RUNTIME_STATE_PATH)


def _read_runtime_state() -> dict[str, Any] | None:
    try:
        data = json.loads(RUNTIME_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _remove_runtime_state(*, controller_pid: int | None = None) -> None:
    if controller_pid is not None:
        state = _read_runtime_state()
        if state and int(state.get("controller_pid") or 0) != controller_pid:
            return
    try:
        RUNTIME_STATE_PATH.unlink()
    except FileNotFoundError:
        pass


def _pid_command(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        command = [
            "powershell",
            "-NoProfile",
            "-Command",
            f'(Get-CimInstance Win32_Process -Filter "ProcessId = {pid}").CommandLine',
        ]
    elif Path(f"/proc/{pid}/cmdline").exists():
        try:
            return (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\0", b" ")
                .decode(errors="replace")
            )
        except OSError:
            return ""
    else:
        command = ["ps", "-p", str(pid), "-o", "command="]

    result = _run(command, capture_output=True, check=False)
    return result.stdout.strip() if result.returncode == 0 else ""


def _pid_matches(pid: int, tokens: Sequence[str]) -> bool:
    command = _pid_command(pid).lower()
    return bool(command) and all(token.lower() in command for token in tokens)


def _terminate_pid(pid: int, tokens: Sequence[str], *, process_group: bool) -> bool:
    if not _pid_matches(pid, tokens):
        return False

    if os.name == "nt":
        _run(
            ["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True
        )
        return True

    try:
        if process_group:
            os.killpg(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    return True


def _wait_until_stopped(pid: int, tokens: Sequence[str], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_matches(pid, tokens):
            return
        time.sleep(0.1)


def _stop_recorded_application() -> bool:
    state = _read_runtime_state()
    if not state:
        _remove_runtime_state()
        return False

    controller_pid = int(state.get("controller_pid") or 0)
    controller_mode = str(state.get("mode") or "launch")
    controller_tokens = ("appctl.py", controller_mode)
    if controller_pid and _terminate_pid(
        controller_pid, controller_tokens, process_group=False
    ):
        _wait_until_stopped(controller_pid, controller_tokens, timeout=12.0)
        if not _pid_matches(controller_pid, controller_tokens):
            _remove_runtime_state()
            return True

    child_entries = (
        ("frontend_pid", ("streamlit", str(FRONTEND_APP)), True),
        ("backend_pid", ("uvicorn", BACKEND_APP), True),
    )
    stopped = False
    for key, tokens, process_group in child_entries:
        pid = int(state.get(key) or 0)
        if pid and _terminate_pid(pid, tokens, process_group=process_group):
            stopped = True
            _wait_until_stopped(pid, tokens)

    if controller_pid and _terminate_pid(
        controller_pid, controller_tokens, process_group=False
    ):
        stopped = True
        _wait_until_stopped(controller_pid, controller_tokens)

    _remove_runtime_state()
    return stopped


def _popen(command: Sequence[str], env: dict[str, str]) -> subprocess.Popen[Any]:
    kwargs: dict[str, Any] = {"cwd": PROJECT_ROOT, "env": env}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(list(command), **kwargs)


def _stop_child(process: subprocess.Popen[Any] | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        if os.name == "nt":
            _run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
            )
        else:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def command_setup(_: argparse.Namespace) -> int:
    with _operation_lock():
        env = _validated_environment(create_env=True)
        _ensure_environment(env)
    print("Preparación completada.")
    return 0


def _prepare_application(
    env: dict[str, str],
    *,
    force_install: bool,
) -> None:
    _ensure_environment(env, force_install=force_install)
    _run([_venv_python(), INIT_DB_SCRIPT], env=env)


def command_update(_: argparse.Namespace) -> int:
    env = _validated_environment(create_env=True)
    with _operation_lock():
        if _stop_recorded_application():
            print("Se ha detenido la aplicación antes de actualizar.")
        result = _attempt_stable_update(env, automatic=False)
        try:
            _prepare_application(
                env,
                force_install=result.dependencies_changed,
            )
        except Exception as exc:
            if result.updated:
                _rollback_update(
                    result,
                    env,
                    reason=f"Actualización revertida durante la preparación: {exc}",
                )
            raise AppCtlError(f"No se pudo preparar la actualización: {exc}") from exc

        completed = _mark_update_success(result, env)
        print(completed.message)
    return 0


def _launch_with_mode(
    *,
    automatic_update: bool,
    reload_backend: bool,
    mode: str,
) -> int:
    env = _validated_environment(create_env=True)
    if mode == "dev" and not str(env.get("APP_CHANNEL") or "").strip():
        env["APP_CHANNEL"] = "develop"

    services: RunningServices | None = None

    def handle_termination(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handle_termination)

    try:
        with _operation_lock():
            if _stop_recorded_application():
                print("Se ha detenido una instancia anterior de la aplicación.")

            update_result: UpdateResult | None = None
            if automatic_update:
                print("Comprobando la versión estable publicada...")
                try:
                    update_result = _attempt_stable_update(env, automatic=True)
                except Exception as exc:
                    update_result = UpdateResult(
                        status="failed",
                        message=(
                            "No se pudo completar la comprobación automática; "
                            f"se mantiene la versión local: {exc}"
                        ),
                    )
                print(update_result.message)

            try:
                _prepare_application(
                    env,
                    force_install=bool(
                        update_result and update_result.dependencies_changed
                    ),
                )
                services = _start_services(
                    env,
                    reload_backend=reload_backend,
                    mode=mode,
                )
            except Exception as exc:
                if not update_result or not update_result.updated:
                    if update_result:
                        _write_update_status(update_result)
                    raise

                reason = (
                    "La versión nueva no pudo arrancar y se ha restaurado la "
                    f"instalación anterior: {exc}"
                )
                print(reason, file=sys.stderr)
                _rollback_update(update_result, env, reason=reason)
                try:
                    _prepare_application(env, force_install=False)
                    services = _start_services(
                        env,
                        reload_backend=reload_backend,
                        mode=mode,
                    )
                except Exception as fallback_exc:
                    raise AppCtlError(
                        "También ha fallado el arranque de la versión restaurada: "
                        f"{fallback_exc}"
                    ) from fallback_exc
            else:
                if update_result:
                    completed = _mark_update_success(update_result, env)
                    if completed.status == "updated":
                        print(completed.message)

        if services is None:
            raise AppCtlError("No se pudieron iniciar los servicios.")
        return _wait_for_services(services)
    except KeyboardInterrupt:
        print("\nDeteniendo la aplicación...")
        return 0
    finally:
        _stop_services(services)


def command_launch(_: argparse.Namespace) -> int:
    return _launch_with_mode(
        automatic_update=True,
        reload_backend=False,
        mode="launch",
    )


def command_dev(_: argparse.Namespace) -> int:
    return _launch_with_mode(
        automatic_update=False,
        reload_backend=True,
        mode="dev",
    )


def command_stop(_: argparse.Namespace) -> int:
    if _stop_recorded_application():
        print("Aplicación detenida.")
    else:
        print("No hay una instancia gestionada por appctl en ejecución.")
    return 0


def _free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def run_smoke(python: Path) -> int:
    env = _validated_environment(create_env=True)
    if not python.exists():
        raise AppCtlError(
            "No existe el entorno virtual; ejecuta setup antes del smoke test."
        )
    missing = _missing_modules(env, python=python)
    if missing:
        raise AppCtlError(
            "No se puede ejecutar el smoke test; faltan dependencias: "
            + ", ".join(missing)
        )

    backend_port = _free_local_port()
    frontend_port = _free_local_port()
    while frontend_port == backend_port:
        frontend_port = _free_local_port()

    services: RunningServices | None = None
    with tempfile.TemporaryDirectory(prefix="media-books-smoke-") as tmp_dir:
        smoke_root = Path(tmp_dir)
        smoke_env = {
            **env,
            "PROJECT_ROOT": str(PROJECT_ROOT),
            "DB_PATH": str(smoke_root / "books.duckdb"),
            "COVERS_DIR": str(smoke_root / "input"),
            "COVERS_OUTPUT_DIR": str(smoke_root / "output" / "covers"),
            "EXPORTS_DIR": str(smoke_root / "output" / "exports"),
            "OCR_OUTPUT_DIR": str(smoke_root / "ocr_output"),
            "BBDD_DIR": str(smoke_root / "bbdd"),
            "SYNC_STATE_PATH": str(smoke_root / "sync_state.json"),
            "BACK_HOST": "127.0.0.1",
            "BACK_PORT": str(backend_port),
            "FRONT_HOST": "127.0.0.1",
            "FRONT_PORT": str(frontend_port),
            "API_URL": f"http://127.0.0.1:{backend_port}",
            "APP_STARTUP_TIMEOUT_SECONDS": "60",
            "STREAMLIT_SERVER_HEADLESS": "true",
            "STREAMLIT_BROWSER_GATHER_USAGE_STATS": "false",
        }
        _run([python, INIT_DB_SCRIPT], env=smoke_env)
        try:
            services = _start_services(
                smoke_env,
                reload_backend=False,
                mode="smoke",
                persist_runtime=False,
                python=python,
            )
            print("Smoke test correcto: backend, frontend y base temporal responden.")
        finally:
            _stop_services(services)
    return 0


def command_smoke(_: argparse.Namespace) -> int:
    return run_smoke(_venv_python())


def _doctor_line(level: str, message: str) -> None:
    print(f"[{level}] {message}")


def _redact_url_credentials(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or parsed.hostname is None or parsed.username is None:
        return value
    hostname = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunsplit(
        (
            parsed.scheme,
            f"***@{hostname}{port}",
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )


def _directory_is_writable(path: Path) -> bool:
    candidate = path.resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate.is_dir() and os.access(candidate, os.W_OK)


def _inspect_database(env: dict[str, str], database_path: Path) -> dict[str, Any]:
    code = (
        "import json, sys; "
        "from src.backend.services.migrations import inspect_status; "
        "print(json.dumps(inspect_status(sys.argv[1])))"
    )
    result = _run(
        [_venv_python(), "-c", code, database_path],
        env=env,
        capture_output=True,
    )
    try:
        status = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AppCtlError(
            "El diagnóstico de DuckDB no devolvió un estado válido."
        ) from exc
    if not isinstance(status, dict):
        raise AppCtlError("El diagnóstico de DuckDB devolvió un estado inesperado.")
    return status


def command_doctor(_: argparse.Namespace) -> int:
    env = _app_environment()
    errors = 0
    warnings = 0
    current_python = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )

    if sys.version_info >= MIN_PYTHON:
        _doctor_line("OK", f"Python {current_python}: {sys.executable}")
    else:
        errors += 1
        _doctor_line("ERROR", f"Python {current_python}; se requiere 3.12 o posterior.")

    git_path = shutil.which("git")
    if git_path:
        _doctor_line("OK", f"Git: {git_path}")
        try:
            current_branch = _git_output(
                ["symbolic-ref", "--quiet", "--short", "HEAD"], env=env
            )
            stable_branch = str(env.get("GIT_BRANCH", "main") or "main").strip()
            level = "OK" if current_branch == stable_branch else "INFO"
            _doctor_line(
                level,
                f"Rama activa: {current_branch}; canal estable: {stable_branch}.",
            )
            dirty = _git_output(
                ["status", "--porcelain", "--untracked-files=all"], env=env
            )
            if dirty:
                warnings += 1
                _doctor_line(
                    "AVISO",
                    "Hay cambios locales; la actualización automática se omitirá.",
                )
            else:
                _doctor_line("OK", "Árbol de trabajo limpio.")
        except AppCtlError as exc:
            warnings += 1
            _doctor_line("AVISO", f"No se pudo inspeccionar el repositorio: {exc}")

        remote = str(env.get("GIT_REMOTE", "origin") or "origin").strip()
        try:
            remote_url = _git_output(["remote", "get-url", remote], env=env)
        except AppCtlError:
            errors += 1
            _doctor_line(
                "ERROR",
                f"No existe el remoto {remote!r}; la actualización automática no funcionará.",
            )
        else:
            _doctor_line(
                "OK", f"Remoto estable {remote}: {_redact_url_credentials(remote_url)}"
            )
    else:
        errors += 1
        _doctor_line("ERROR", "Git no está disponible; la actualización no funcionará.")

    if ENV_PATH.exists():
        _doctor_line("OK", f"Configuración local: {ENV_PATH}")
    else:
        warnings += 1
        _doctor_line("AVISO", "No existe .env; setup la creará desde .env.example.")

    for issue in _configuration_issues(env):
        if issue.level == "ERROR":
            errors += 1
        else:
            warnings += 1
        _doctor_line(issue.level, issue.message)

    python = _venv_python()
    dependencies_ready = False
    if python.exists():
        missing = _missing_modules(env)
        if missing:
            errors += 1
            _doctor_line("ERROR", f"Dependencias ausentes: {', '.join(missing)}")
        else:
            dependencies_ready = True
            _doctor_line("OK", f"Entorno virtual: {VENV_DIR}")
            try:
                desired_state = _desired_dependency_state()
            except AppCtlError as exc:
                errors += 1
                _doctor_line("ERROR", str(exc))
            else:
                if _read_dependency_state() != desired_state:
                    errors += 1
                    _doctor_line(
                        "ERROR",
                        "El entorno no coincide con pyproject.toml y requirements.lock; ejecuta setup.",
                    )
                else:
                    _doctor_line("OK", "Dependencias sincronizadas con el lock.")
    else:
        errors += 1
        _doctor_line("ERROR", "No existe el entorno virtual; ejecuta setup.")

    iso_table = PROJECT_ROOT / "assets" / "iso-639-3.tab"
    icon = PROJECT_ROOT / "assets" / "dani.png"
    try:
        iso_header = iso_table.open("r", encoding="utf-8").readline().rstrip("\r\n")
    except (OSError, UnicodeError):
        iso_header = ""
    if iso_header == (
        "Id\tPart2b\tPart2t\tPart1\tScope\tLanguage_Type\tRef_Name\tComment"
    ):
        _doctor_line("OK", f"Tabla ISO 639-3: {iso_table}")
    else:
        errors += 1
        _doctor_line(
            "ERROR", "Falta una tabla assets/iso-639-3.tab válida; ejecuta setup."
        )
    try:
        png_signature = icon.open("rb").read(8)
    except OSError:
        png_signature = b""
    if png_signature == b"\x89PNG\r\n\x1a\n":
        _doctor_line("OK", f"Icono local: {icon}")
    elif icon.exists():
        warnings += 1
        _doctor_line(
            "AVISO",
            "assets/dani.png no es un PNG válido; se usará el icono predeterminado.",
        )
    else:
        warnings += 1
        _doctor_line("AVISO", "No hay icono local; Streamlit usará el predeterminado.")

    database_path = _resolve_app_path(env, "DB_PATH", "data/books.duckdb")
    path_contracts = (
        ("base de datos", database_path.parent),
        (
            "portadas de salida",
            _resolve_app_path(env, "COVERS_OUTPUT_DIR", "data/output/covers"),
        ),
        ("exportaciones", _resolve_app_path(env, "EXPORTS_DIR", "data/output/exports")),
        ("snapshots", _resolve_app_path(env, "BBDD_DIR", "../bbdd")),
    )
    for label, path in path_contracts:
        if _directory_is_writable(path):
            _doctor_line("OK", f"Ruta escribible para {label}: {path}")
        else:
            errors += 1
            _doctor_line("ERROR", f"No se puede escribir en la ruta de {label}: {path}")

    covers_input = _resolve_app_path(env, "COVERS_DIR", "data/input")
    if covers_input.is_dir() and os.access(covers_input, os.R_OK):
        _doctor_line("OK", f"Entrada de portadas accesible: {covers_input}")
    elif covers_input.exists():
        errors += 1
        _doctor_line(
            "ERROR", f"No se puede leer la entrada de portadas: {covers_input}"
        )
    else:
        warnings += 1
        _doctor_line("AVISO", f"La entrada de portadas aún no existe: {covers_input}")

    if database_path.exists() and dependencies_ready:
        try:
            database_status = _inspect_database(env, database_path)
        except (AppCtlError, OSError) as exc:
            errors += 1
            _doctor_line("ERROR", f"No se pudo inspeccionar DuckDB: {exc}")
        else:
            pending = list(database_status.get("pending_versions") or [])
            if pending:
                warnings += 1
                _doctor_line(
                    "AVISO",
                    "DuckDB tiene migraciones pendientes que se aplicarán al arrancar: "
                    + ", ".join(str(item) for item in pending),
                )
            else:
                _doctor_line(
                    "OK",
                    f"DuckDB en esquema {database_status.get('schema_version')}: {database_path}",
                )
    elif not database_path.exists():
        _doctor_line("INFO", f"DuckDB se creará en el primer arranque: {database_path}")

    state = _read_runtime_state()
    controller_pid = int(state.get("controller_pid") or 0) if state else 0
    controller_mode = str(state.get("mode") or "launch") if state else "launch"
    if controller_pid and _pid_matches(controller_pid, ("appctl.py", controller_mode)):
        back_host = str(env.get("BACK_HOST", "127.0.0.1") or "127.0.0.1")
        front_host = str(env.get("FRONT_HOST", "127.0.0.1") or "127.0.0.1")
        back_url = _service_url(back_host, _port(env, "BACK_PORT", 8000), "/health")
        front_url = _service_url(
            front_host, _port(env, "FRONT_PORT", 8501), "/_stcore/health"
        )
        if _http_is_ready(back_url) and _http_is_ready(front_url):
            _doctor_line(
                "OK",
                f"Aplicación en ejecución y saludable ({controller_mode}); PID {controller_pid}.",
            )
        else:
            errors += 1
            _doctor_line(
                "ERROR",
                f"La aplicación figura en ejecución, pero no responde; PID {controller_pid}.",
            )
    else:
        _doctor_line("INFO", "Aplicación detenida.")
        for name, host_key, port_key, default in (
            ("backend", "BACK_HOST", "BACK_PORT", 8000),
            ("frontend", "FRONT_HOST", "FRONT_PORT", 8501),
        ):
            host = str(env.get(host_key, "127.0.0.1") or "127.0.0.1")
            try:
                port = _port(env, port_key, default)
            except AppCtlError:
                continue
            if _port_is_available(host, port):
                _doctor_line("OK", f"Puerto de {name} disponible: {host}:{port}")
            else:
                errors += 1
                _doctor_line(
                    "ERROR",
                    f"Puerto de {name} ocupado por otro proceso: {host}:{port}",
                )

    active_providers = {
        str(env.get("OCR_PROVIDER", "ollama") or "ollama").strip().lower(),
        str(env.get("CATALOG_PROVIDER", "ollama") or "ollama").strip().lower(),
        str(env.get("CATALOG_ARBITER_PROVIDER", "auto") or "auto").strip().lower(),
    }
    if "ollama" in active_providers or "auto" in active_providers:
        ollama_url = str(
            env.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
            or "http://127.0.0.1:11434"
        ).rstrip("/")
        if _http_is_ready(f"{ollama_url}/api/tags"):
            _doctor_line("OK", f"Ollama responde en {ollama_url}.")
        else:
            warnings += 1
            _doctor_line(
                "AVISO",
                f"Ollama no responde en {ollama_url}; inicia Ollama antes de usar esos flujos.",
            )

    if shutil.which("make"):
        _doctor_line(
            "INFO", "GNU Make está disponible, pero sólo es opcional para desarrollo."
        )
    else:
        _doctor_line("OK", "GNU Make no es necesario para los lanzadores.")

    if errors:
        _doctor_line(
            "RESUMEN",
            f"Diagnóstico con {errors} error(es) y {warnings} aviso(s).",
        )
    else:
        _doctor_line("RESUMEN", f"Instalación preparada; {warnings} aviso(s).")
    return 1 if errors else 0


def command_update_and_launch(args: argparse.Namespace) -> int:
    return command_launch(args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Control multiplataforma de Media Catalog Books."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    commands = {
        "setup": (command_setup, "Prepara recursos, entorno y dependencias."),
        "launch": (
            command_launch,
            "Actualiza desde el canal estable y arranca la aplicación.",
        ),
        "dev": (command_dev, "Arranca en desarrollo sin actualizar."),
        "update": (
            command_update,
            "Actualiza de forma explícita desde el canal estable.",
        ),
        "stop": (command_stop, "Detiene los procesos gestionados por appctl."),
        "doctor": (command_doctor, "Comprueba requisitos y estado local."),
        "smoke": (
            command_smoke,
            "Arranca backend y frontend con datos temporales y comprueba su salud.",
        ),
        "update-and-launch": (
            command_update_and_launch,
            "Actualiza la aplicación y después la arranca.",
        ),
    }
    for name, (handler, help_text) in commands.items():
        command_parser = subparsers.add_parser(name, help=help_text)
        command_parser.set_defaults(handler=handler)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _require_python_version()
        return int(args.handler(args))
    except AppCtlError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
