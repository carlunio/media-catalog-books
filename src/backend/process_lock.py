from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TextIO


class DatabaseProcessLockError(RuntimeError):
    pass


class DatabaseProcessLock:
    def __init__(self, database_path: str | Path) -> None:
        database = Path(database_path).resolve()
        self.database_path = database
        self.lock_path = database.with_name(f"{database.name}.process.lock")
        self._handle: TextIO | None = None
        self._locked = False

    def acquire(self) -> Path:
        if self._handle is not None:
            return self.lock_path

        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            self._lock_handle(handle)
        except OSError as exc:
            owner = self._read_owner(handle)
            handle.close()
            owner_text = f" (PID {owner})" if owner else ""
            raise DatabaseProcessLockError(
                "Otra instancia del backend está usando la misma base de datos"
                f"{owner_text}: {self.database_path}. Detén la instancia anterior "
                "antes de arrancar otra."
            ) from exc

        handle.seek(0)
        handle.truncate()
        handle.write(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "database": str(self.database_path),
                }
            )
            + chr(10)
        )
        handle.flush()
        self._handle = handle
        self._locked = True
        return self.lock_path

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            if self._locked:
                self._unlock_handle(handle)
        finally:
            handle.close()
            self._handle = None
            self._locked = False

    @staticmethod
    def _read_owner(handle: TextIO) -> int | None:
        try:
            handle.seek(0)
            payload = json.loads(handle.read() or "{}")
            pid = int(payload.get("pid") or 0)
        except (OSError, ValueError, TypeError):
            return None
        return pid if pid > 0 else None

    @staticmethod
    def _lock_handle(handle: TextIO) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(chr(0))
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock_handle(handle: TextIO) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
