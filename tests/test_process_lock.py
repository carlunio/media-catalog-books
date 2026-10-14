import subprocess
import sys

from src.backend.process_lock import DatabaseProcessLock


def test_database_process_lock_rejects_a_second_backend(tmp_path):
    database_path = tmp_path / "books.duckdb"
    first = DatabaseProcessLock(database_path)
    first.acquire()
    code = """
import sys

from src.backend.process_lock import DatabaseProcessLock, DatabaseProcessLockError

lock = DatabaseProcessLock(sys.argv[1])
try:
    lock.acquire()
except DatabaseProcessLockError as exc:
    print(exc)
    raise SystemExit(3)
raise SystemExit(0)
"""
    try:
        second = subprocess.run(
            [sys.executable, "-c", code, str(database_path)],
            check=False,
            capture_output=True,
            text=True,
        )
    finally:
        first.release()

    assert second.returncode == 3
    assert "Otra instancia del backend" in second.stdout

    replacement = DatabaseProcessLock(database_path)
    replacement.acquire()
    replacement.release()
