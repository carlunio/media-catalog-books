from pathlib import Path

import duckdb

from .config import DB_PATH
from .language_codes import idioma_es_a_iso639_3


def get_connection(
    database_path: str | Path | None = None,
    *,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    target_path = Path(database_path) if database_path is not None else DB_PATH
    if not read_only:
        target_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(target_path), read_only=read_only)
    if read_only:
        return con
    try:
        con.create_function(
            "idioma_es_a_iso639_3",
            idioma_es_a_iso639_3,
            return_type=duckdb.sqltypes.VARCHAR,
        )
    except Exception:
        # Function registration is best-effort; queries can still run without it.
        pass
    return con
