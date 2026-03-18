import duckdb

from .config import DB_PATH
from .language_codes import idioma_es_a_iso639_3


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
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
