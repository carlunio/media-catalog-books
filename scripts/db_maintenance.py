from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def _file_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    return path.stat().st_size / (1024 * 1024)


def _prune_catalog_raw_output(con: duckdb.DuckDBPyConnection) -> int:
    try:
        rows = con.execute(
            """
            SELECT book_id, CAST(payload_json AS VARCHAR) AS payload_text
            FROM book_payloads
            WHERE payload_type = 'catalog'
            """
        ).fetchall()
    except Exception:
        return 0

    updates: list[tuple[str, str]] = []
    for row in rows:
        book_id = str(row[0] or "").strip()
        payload_text = str(row[1] or "").strip()
        if not book_id or not payload_text:
            continue
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if "raw_llm_output" not in payload:
            continue
        payload.pop("raw_llm_output", None)
        updates.append((json.dumps(payload, ensure_ascii=False), book_id))

    if not updates:
        return 0

    con.executemany(
        """
        UPDATE book_payloads
        SET payload_json = ?::JSON
        WHERE payload_type = 'catalog' AND book_id = ?
        """,
        updates,
    )
    return len(updates)


def main() -> int:
    parser = argparse.ArgumentParser(description="DuckDB maintenance for media-catalog-books")
    parser.add_argument(
        "--db",
        default="data/books.duckdb",
        help="Path to DuckDB file (default: data/books.duckdb)",
    )
    parser.add_argument(
        "--skip-prune-catalog-raw",
        action="store_true",
        help="Skip removal of legacy raw_llm_output from catalog payloads",
    )
    parser.add_argument(
        "--repack",
        action="store_true",
        help="Create a compact rebuilt copy as <db>.repacked.duckdb",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace original DB with repacked copy (requires --repack)",
    )
    args = parser.parse_args()
    if args.replace and not args.repack:
        raise SystemExit("--replace requires --repack")

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    before_mb = _file_size_mb(db_path)
    print(f"DB: {db_path}")
    print(f"Size before: {before_mb:.2f} MB")

    with duckdb.connect(str(db_path)) as con:
        removed = 0
        if not args.skip_prune_catalog_raw:
            removed = _prune_catalog_raw_output(con)
            print(f"Catalog payloads cleaned (raw_llm_output removed): {removed}")

        # Persist old versions to stable storage and then rewrite file compactly.
        con.execute("CHECKPOINT")
        con.execute("VACUUM")

        try:
            info = con.execute("PRAGMA database_size").fetchall()
            if info:
                print(f"PRAGMA database_size: {info[0]}")
        except Exception:
            pass

    after_mb = _file_size_mb(db_path)
    delta_mb = after_mb - before_mb
    print(f"Size after: {after_mb:.2f} MB")
    print(f"Delta: {delta_mb:+.2f} MB")

    repacked_path: Path | None = None
    if args.repack:
        repacked_path = db_path.with_suffix(".repacked.duckdb")
        if repacked_path.exists():
            repacked_path.unlink()

        with duckdb.connect(str(db_path)) as con:
            db_list = con.execute("PRAGMA database_list").fetchall()
            if not db_list:
                raise SystemExit("Unable to resolve current DuckDB catalog name for repack")
            catalog_name = str(db_list[0][1])
            con.execute(f"ATTACH '{repacked_path.as_posix()}' AS repacked")
            con.execute(f'COPY FROM DATABASE "{catalog_name}" TO repacked')
            con.execute("DETACH repacked")

        repacked_mb = _file_size_mb(repacked_path)
        print(f"Repacked copy: {repacked_path}")
        print(f"Repacked size: {repacked_mb:.2f} MB")

    if args.replace and repacked_path is not None:
        if not repacked_path.exists():
            raise SystemExit(f"Repacked file not found: {repacked_path}")
        backup_path = db_path.with_suffix(".pre_repack.bak.duckdb")
        if backup_path.exists():
            backup_path.unlink()
        db_path.replace(backup_path)
        repacked_path.replace(db_path)
        final_mb = _file_size_mb(db_path)
        print(f"Original DB moved to backup: {backup_path}")
        print(f"Replacement complete. New DB size: {final_mb:.2f} MB")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
