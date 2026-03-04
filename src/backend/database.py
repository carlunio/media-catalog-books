import duckdb

from .config import DB_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(DB_PATH)
