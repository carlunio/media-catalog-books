from pathlib import Path

from scripts import lock_dependencies


def _configure_lock_check(tmp_path: Path, monkeypatch, resolved: bytes) -> Path:
    lock_path = tmp_path / "requirements.lock"
    monkeypatch.setattr(lock_dependencies, "LOCK_PATH", lock_path)

    def compile_candidate(output_path: Path, *, upgrade: bool) -> None:
        assert upgrade is False
        output_path.write_bytes(resolved)

    monkeypatch.setattr(lock_dependencies, "_compile", compile_candidate)
    return lock_path


def test_check_lock_ignores_checkout_line_endings(tmp_path, monkeypatch):
    lock_path = _configure_lock_check(
        tmp_path,
        monkeypatch,
        b"package==1.0 \\\n    --hash=sha256:abc\n",
    )
    lock_path.write_bytes(b"package==1.0 \\\r\n    --hash=sha256:abc\r\n")

    assert lock_dependencies._check_lock() == 0


def test_check_lock_rejects_a_resolved_content_change(tmp_path, monkeypatch, capsys):
    lock_path = _configure_lock_check(
        tmp_path,
        monkeypatch,
        b"package==2.0 \\\n    --hash=sha256:def\n",
    )
    lock_path.write_bytes(b"package==1.0 \\\r\n    --hash=sha256:abc\r\n")

    assert lock_dependencies._check_lock() == 1
    assert "no coincide con pyproject.toml" in capsys.readouterr().err
