import subprocess
import socket
from pathlib import Path

import pytest

from scripts import appctl


def test_read_dotenv_supports_quotes_export_and_ignores_invalid_lines(tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        """
# comment
PLAIN=value
export QUOTED="value with spaces"
SINGLE='other value'
INVALID LINE
1INVALID=no
""".strip(),
        encoding="utf-8",
    )

    assert appctl._read_dotenv(env_path) == {
        "PLAIN": "value",
        "QUOTED": "value with spaces",
        "SINGLE": "other value",
    }


def test_ensure_env_file_creates_once_without_overwriting(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    example_path = tmp_path / ".env.example"
    example_path.write_text("BACK_PORT=8000\n", encoding="utf-8")
    monkeypatch.setattr(appctl, "ENV_PATH", env_path)
    monkeypatch.setattr(appctl, "ENV_EXAMPLE_PATH", example_path)

    assert appctl._ensure_env_file() is True
    assert env_path.read_text(encoding="utf-8") == "BACK_PORT=8000\n"

    env_path.write_text("BACK_PORT=8123\n", encoding="utf-8")
    assert appctl._ensure_env_file() is False
    assert env_path.read_text(encoding="utf-8") == "BACK_PORT=8123\n"


def test_app_environment_normalizes_root_and_derives_api_url(tmp_path, monkeypatch):
    monkeypatch.setattr(appctl, "ENV_PATH", tmp_path / "missing.env")
    monkeypatch.delenv("PROJECT_ROOT", raising=False)
    monkeypatch.delenv("API_URL", raising=False)
    monkeypatch.setenv("BACK_PORT", "8123")

    env = appctl._app_environment()

    assert env["PROJECT_ROOT"] == str(appctl.PROJECT_ROOT.resolve())
    assert env["API_URL"] == "http://127.0.0.1:8123"


def test_env_example_is_a_keyless_local_first_configuration():
    values = appctl._read_dotenv(appctl.ENV_EXAMPLE_PATH)

    assert values["PROJECT_ROOT"] == "."
    assert values["API_URL"] == ""
    assert values["OPENAI_API_KEY"] == ""
    assert values["ISBNDB_API_KEY"] == ""
    assert values["SYNC_ACTOR"] == ""
    assert values["SYNC_DEVICE"] == ""
    assert values["OCR_PROVIDER"] == "ollama"
    assert values["CATALOG_PROVIDER"] == "ollama"


def test_remote_url_credentials_are_redacted():
    assert (
        appctl._redact_url_credentials("https://user:secret@example.com/repo.git")
        == "https://***@example.com/repo.git"
    )
    assert (
        appctl._redact_url_credentials("git@github.com:owner/repo.git")
        == "git@github.com:owner/repo.git"
    )


def test_configuration_validation_reports_actionable_errors(tmp_path):
    env_path = tmp_path / ".env"
    example_path = tmp_path / ".env.example"
    env_path.write_text(
        "BROKEN LINE\nUNKNOWN=value\nUNKNOWN=again\n",
        encoding="utf-8",
    )
    example_path.write_text("BACK_PORT=8000\n", encoding="utf-8")
    env = {
        "PROJECT_ROOT": str(appctl.PROJECT_ROOT),
        "BACK_PORT": "8000",
        "FRONT_PORT": "8000",
        "API_URL": "not-a-url",
        "OCR_PROVIDER": "invalid",
        "OCR_USE_SIDECAR": "perhaps",
    }

    issues = appctl._configuration_issues(
        env,
        env_path=env_path,
        example_path=example_path,
    )
    messages = "\n".join(issue.message for issue in issues)

    assert "falta '='" in messages
    assert "ya estaba definida" in messages
    assert "Claves no reconocidas" in messages
    assert "puertos diferentes" in messages
    assert "API_URL debe ser una URL HTTP válida" in messages
    assert "OCR_PROVIDER debe ser uno de" in messages
    assert "OCR_USE_SIDECAR debe ser true o false" in messages


def test_service_port_preflight_detects_an_occupied_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        occupied_port = int(listener.getsockname()[1])

        with pytest.raises(appctl.AppCtlError, match="puerto ya está ocupado"):
            appctl._ensure_service_ports_available(
                {
                    "BACK_HOST": "127.0.0.1",
                    "BACK_PORT": str(occupied_port),
                    "FRONT_HOST": "127.0.0.1",
                    "FRONT_PORT": str(appctl._free_local_port()),
                }
            )


def test_service_commands_use_configured_ports_and_managed_python():
    backend, frontend = appctl._service_commands(
        {"BACK_PORT": "8123", "FRONT_PORT": "8567"}, reload_backend=True
    )

    assert backend[0] == str(appctl._venv_python())
    assert backend[backend.index("--host") + 1] == "127.0.0.1"
    assert backend[backend.index("--port") + 1] == "8123"
    assert backend[-1] == "--reload"
    assert frontend[0] == str(appctl._venv_python())
    assert frontend[frontend.index("--server.address") + 1] == "127.0.0.1"
    assert frontend[frontend.index("--server.port") + 1] == "8567"


def test_stable_service_command_disables_reload():
    backend, _frontend = appctl._service_commands({}, reload_backend=False)

    assert "--reload" not in backend


def test_appctl_exposes_all_user_commands():
    parser = appctl._build_parser()

    commands = (
        "setup",
        "launch",
        "dev",
        "update",
        "stop",
        "doctor",
        "smoke",
        "update-and-launch",
    )
    for command in commands:
        assert parser.parse_args([command]).command == command


def test_user_launchers_delegate_to_appctl_without_make():
    tools_dir = appctl.PROJECT_ROOT / "tools"
    launchers = (
        "launch-app.sh",
        "set-up-app.sh",
        "stop-app.sh",
        "update-app.sh",
        "update-and-launch-app.sh",
        "launch-app.bat",
        "set-up-app.bat",
        "stop-app.bat",
        "update-app.bat",
        "update-and-launch-app.bat",
    )

    for filename in launchers:
        content = (tools_dir / filename).read_text(encoding="utf-8").lower()
        assert "appctl" in content
        assert "make" not in content


def test_release_fingerprint_includes_requirements_lock(tmp_path):
    pyproject_path = tmp_path / "pyproject.toml"
    lock_path = tmp_path / "requirements.lock"
    pyproject_path.write_text(
        '[project]\nname = "fixture"\nversion = "1.0.0"\ndependencies = []\n',
        encoding="utf-8",
    )
    lock_path.write_text("example==1.0\n", encoding="utf-8")

    version_before, fingerprint_before = appctl._project_release_data(pyproject_path)
    lock_path.write_text("example==1.1\n", encoding="utf-8")
    version_after, fingerprint_after = appctl._project_release_data(pyproject_path)

    assert version_before == version_after == "1.0.0"
    assert fingerprint_before != fingerprint_after


def test_locked_install_uses_hashes_and_installs_project_without_dependencies(
    tmp_path, monkeypatch
):
    project_root = tmp_path / "project"
    venv_dir = project_root / ".venv"
    project_root.mkdir()
    venv_dir.mkdir()
    pyproject_path = project_root / "pyproject.toml"
    lock_path = project_root / "requirements.lock"
    pyproject_path.write_text('[project]\nname = "fixture"\n', encoding="utf-8")
    lock_path.write_text("example==1.0\n", encoding="utf-8")
    calls: list[list[str]] = []

    monkeypatch.setattr(appctl, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(appctl, "VENV_DIR", venv_dir)
    monkeypatch.setattr(appctl, "PYPROJECT_PATH", pyproject_path)
    monkeypatch.setattr(appctl, "LOCK_PATH", lock_path)
    monkeypatch.setattr(
        appctl,
        "_run",
        lambda command, **_kwargs: (
            calls.append([str(part) for part in command])
            or subprocess.CompletedProcess(command, 0, "", "")
        ),
    )

    appctl._install_dependencies({})

    python = str(appctl._venv_python())
    assert calls == [
        [
            python,
            "-m",
            "pip",
            "install",
            "--require-hashes",
            "--requirement",
            str(lock_path),
        ],
        [
            python,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--no-build-isolation",
            "--editable",
            str(project_root),
        ],
        [python, "-m", "pip", "check"],
    ]
    assert appctl._read_dependency_state() == appctl._desired_dependency_state()


def test_environment_is_rebuilt_when_lock_state_is_missing(tmp_path, monkeypatch):
    project_root = tmp_path / "project"
    venv_dir = project_root / ".venv"
    python_path = venv_dir / (
        "Scripts/python.exe" if appctl.os.name == "nt" else "bin/python"
    )
    python_path.parent.mkdir(parents=True)
    python_path.write_text("old environment", encoding="utf-8")
    sentinel = venv_dir / "obsolete-package.txt"
    sentinel.write_text("obsolete", encoding="utf-8")
    pyproject_path = project_root / "pyproject.toml"
    lock_path = project_root / "requirements.lock"
    pyproject_path.write_text('[project]\nname = "fixture"\n', encoding="utf-8")
    lock_path.write_text("example==1.0\n", encoding="utf-8")
    installs: list[bool] = []

    monkeypatch.setattr(appctl, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(appctl, "VENV_DIR", venv_dir)
    monkeypatch.setattr(appctl, "PYPROJECT_PATH", pyproject_path)
    monkeypatch.setattr(appctl, "LOCK_PATH", lock_path)
    monkeypatch.setattr(appctl, "_prepare_assets", lambda _env: None)
    monkeypatch.setattr(
        appctl,
        "_create_venv",
        lambda _env: appctl._venv_python().parent.mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(appctl, "_missing_modules", lambda _env: [])
    monkeypatch.setattr(
        appctl, "_install_dependencies", lambda _env: installs.append(True)
    )

    appctl._ensure_environment({})

    assert installs == [True]
    assert not sentinel.exists()


def test_stop_requests_a_coordinated_controller_shutdown_first(monkeypatch):
    calls: list[tuple[int, tuple[str, ...], bool]] = []
    matches = iter((False,))

    monkeypatch.setattr(
        appctl,
        "_read_runtime_state",
        lambda: {"controller_pid": 10, "frontend_pid": 20, "backend_pid": 30},
    )
    monkeypatch.setattr(
        appctl,
        "_terminate_pid",
        lambda pid, tokens, process_group: (
            calls.append((pid, tuple(tokens), process_group)) or True
        ),
    )
    monkeypatch.setattr(appctl, "_wait_until_stopped", lambda *args, **kwargs: None)
    monkeypatch.setattr(appctl, "_pid_matches", lambda *args, **kwargs: next(matches))
    monkeypatch.setattr(appctl, "_remove_runtime_state", lambda **kwargs: None)

    assert appctl._stop_recorded_application() is True
    assert calls == [(10, ("appctl.py", "launch"), False)]


def test_automatic_update_skips_a_non_stable_branch(monkeypatch):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "develop"
        raise AssertionError(arguments)

    monkeypatch.setattr(appctl, "_git_output", git_output)

    result = appctl._attempt_stable_update({}, automatic=True)

    assert result.status == "skipped"
    assert "develop" in result.message


def test_explicit_update_rejects_a_non_stable_branch(monkeypatch):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "develop"
        raise AssertionError(arguments)

    monkeypatch.setattr(appctl, "_git_output", git_output)

    with pytest.raises(appctl.AppCtlError, match="rama activa"):
        appctl._attempt_stable_update({}, automatic=False)


def test_automatic_update_skips_a_dirty_stable_installation(monkeypatch):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "main"
        if arguments[0] == "status":
            return " M README.md"
        raise AssertionError(arguments)

    monkeypatch.setattr(appctl, "_git_output", git_output)

    result = appctl._attempt_stable_update({}, automatic=True)

    assert result.status == "skipped"
    assert "modificados" in result.message


def test_automatic_update_uses_local_version_when_fetch_fails(monkeypatch):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "main"
        if arguments[0] == "status":
            return ""
        if arguments[:2] == ["rev-parse", "HEAD"]:
            return "old-revision"
        raise AssertionError(arguments)

    monkeypatch.setattr(appctl, "_git_output", git_output)
    monkeypatch.setattr(
        appctl, "_project_release_data", lambda: ("1.2.3", "dependencies")
    )
    monkeypatch.setattr(
        appctl,
        "_run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            appctl.AppCtlError("sin conexión")
        ),
    )

    result = appctl._attempt_stable_update({}, automatic=True)

    assert result.status == "offline"
    assert result.old_revision == result.new_revision == "old-revision"
    assert result.new_version == "1.2.3"


def test_stable_update_applies_only_fast_forward_and_detects_dependencies(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "main"
        if arguments[0] == "status":
            return ""
        if arguments[:2] == ["rev-parse", "HEAD"]:
            return "old-revision"
        if arguments[:2] == ["rev-parse", "FETCH_HEAD"]:
            return "new-revision"
        raise AssertionError(arguments)

    commands: list[list[str]] = []

    def run(command, **_kwargs):
        commands.append([str(part) for part in command])
        return subprocess.CompletedProcess(command, 0, "", "")

    release_data = iter((("1.2.3", "old-deps"), ("1.3.0", "new-deps")))
    database_path = tmp_path / "books.duckdb"
    backup_path = tmp_path / "backup.duckdb"
    monkeypatch.setattr(appctl, "_git_output", git_output)
    monkeypatch.setattr(appctl, "_git_is_ancestor", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(appctl, "_run", run)
    monkeypatch.setattr(appctl, "_project_release_data", lambda: next(release_data))
    monkeypatch.setattr(
        appctl,
        "_backup_database",
        lambda *_args, **_kwargs: (database_path, backup_path, True),
    )
    monkeypatch.setattr(appctl, "_write_update_status", lambda _result: None)

    result = appctl._attempt_stable_update({}, automatic=True)

    assert result.updated is True
    assert result.old_revision == "old-revision"
    assert result.new_revision == "new-revision"
    assert result.old_version == "1.2.3"
    assert result.new_version == "1.3.0"
    assert result.dependencies_changed is True
    assert ["git", "merge", "--ff-only", "new-revision"] in commands


def test_automatic_update_keeps_local_version_when_backup_fails(monkeypatch):
    monkeypatch.setattr(appctl.shutil, "which", lambda _command: "/usr/bin/git")

    def git_output(arguments, **_kwargs):
        if arguments[:2] == ["rev-parse", "--is-inside-work-tree"]:
            return "true"
        if arguments[0] == "symbolic-ref":
            return "main"
        if arguments[0] == "status":
            return ""
        if arguments[:2] == ["rev-parse", "HEAD"]:
            return "old-revision"
        if arguments[:2] == ["rev-parse", "FETCH_HEAD"]:
            return "new-revision"
        raise AssertionError(arguments)

    commands: list[list[str]] = []

    def run(command, **_kwargs):
        commands.append([str(part) for part in command])
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(appctl, "_git_output", git_output)
    monkeypatch.setattr(appctl, "_git_is_ancestor", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(appctl, "_run", run)
    monkeypatch.setattr(
        appctl, "_project_release_data", lambda: ("1.2.3", "dependencies")
    )
    monkeypatch.setattr(
        appctl,
        "_backup_database",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disco lleno")),
    )

    result = appctl._attempt_stable_update({}, automatic=True)

    assert result.status == "failed"
    assert result.old_revision == result.new_revision == "old-revision"
    assert result.old_version == result.new_version == "1.2.3"
    assert not any(command[:2] == ["git", "merge"] for command in commands)


def test_rollback_restores_code_and_database(tmp_path, monkeypatch):
    database_path = tmp_path / "books.duckdb"
    backup_path = tmp_path / "backup.duckdb"
    database_path.write_text("new", encoding="utf-8")
    backup_path.write_text("old", encoding="utf-8")
    commands: list[list[str]] = []

    monkeypatch.setattr(
        appctl,
        "_run",
        lambda command, **_kwargs: (
            commands.append([str(part) for part in command])
            or subprocess.CompletedProcess(command, 0, "", "")
        ),
    )
    monkeypatch.setattr(appctl, "_write_update_status", lambda _result: None)
    result = appctl.UpdateResult(
        status="updated_pending",
        message="pending",
        old_revision="old-revision",
        new_revision="new-revision",
        old_version="1.2.3",
        new_version="1.3.0",
        database_path=str(database_path),
        database_backup=str(backup_path),
        database_existed=True,
    )

    rolled_back = appctl._rollback_update(result, {}, reason="startup failed")

    assert database_path.read_text(encoding="utf-8") == "old"
    assert rolled_back.status == "rolled_back"
    assert rolled_back.new_revision == "old-revision"
    assert ["git", "reset", "--hard", "old-revision"] in commands


def test_launch_and_dev_use_separate_modes(monkeypatch):
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        appctl,
        "_launch_with_mode",
        lambda **kwargs: calls.append(kwargs) or 0,
    )

    assert appctl.command_launch(None) == 0
    assert appctl.command_dev(None) == 0
    assert calls == [
        {
            "automatic_update": True,
            "reload_backend": False,
            "mode": "launch",
        },
        {
            "automatic_update": False,
            "reload_backend": True,
            "mode": "dev",
        },
    ]


def test_stable_update_fast_forwards_from_a_local_remote(tmp_path, monkeypatch):
    source = tmp_path / "source"
    remote = tmp_path / "remote.git"
    install = tmp_path / "install"

    def git(cwd: Path, *arguments: str) -> str:
        result = subprocess.run(
            ["git", *arguments],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.email", "tests@example.invalid")
    git(source, "config", "user.name", "Tests")
    (source / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "1.0.0"\ndependencies = []\n',
        encoding="utf-8",
    )
    (source / ".gitignore").write_text("/data/\n.runtime/\n", encoding="utf-8")
    (source / "marker.txt").write_text("one", encoding="utf-8")
    git(source, "add", ".")
    git(source, "commit", "-m", "initial")

    git(tmp_path, "init", "--bare", str(remote))
    git(source, "remote", "add", "origin", str(remote))
    git(source, "push", "-u", "origin", "main")
    git(tmp_path, "clone", str(remote), str(install))

    (source / "marker.txt").write_text("two", encoding="utf-8")
    (source / "pyproject.toml").write_text(
        '[project]\nname = "fixture"\nversion = "1.1.0"\ndependencies = []\n',
        encoding="utf-8",
    )
    git(source, "add", ".")
    git(source, "commit", "-m", "release")
    git(source, "push", "origin", "main")

    database_path = install / "data" / "books.duckdb"
    database_path.parent.mkdir(parents=True)
    database_path.write_bytes(b"database-before-update")

    original_release_data = appctl._project_release_data
    monkeypatch.setattr(appctl, "PROJECT_ROOT", install)
    monkeypatch.setattr(appctl, "VENV_DIR", install / ".venv")
    monkeypatch.setattr(appctl, "RUNTIME_DIR", install / ".runtime")
    monkeypatch.setattr(
        appctl, "UPDATE_STATUS_PATH", install / ".runtime" / "last-update.json"
    )
    monkeypatch.setattr(appctl, "PYPROJECT_PATH", install / "pyproject.toml")
    monkeypatch.setattr(
        appctl,
        "_project_release_data",
        lambda: original_release_data(install / "pyproject.toml"),
    )

    result = appctl._attempt_stable_update(
        {
            "PROJECT_ROOT": str(install),
            "DB_PATH": "data/books.duckdb",
            "GIT_REMOTE": "origin",
            "GIT_BRANCH": "main",
        },
        automatic=False,
    )

    assert result.updated is True
    assert result.old_version == "1.0.0"
    assert result.new_version == "1.1.0"
    assert result.dependencies_changed is False
    assert (install / "marker.txt").read_text(encoding="utf-8") == "two"
    assert git(install, "rev-parse", "HEAD") == git(source, "rev-parse", "HEAD")
    assert result.database_backup is not None
    assert Path(result.database_backup).read_bytes() == b"database-before-update"
