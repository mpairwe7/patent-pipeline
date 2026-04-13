"""Load stage — create the DuckDB warehouse and populate it from clean CSVs."""

from __future__ import annotations

from pathlib import Path

import duckdb

from patent_pipeline.config import Settings
from patent_pipeline.logging_setup import logger

TABLE_TO_FILE = {
    "patents": "clean_patents.csv",
    "inventors": "clean_inventors.csv",
    "companies": "clean_companies.csv",
    "patent_relationships": "clean_relationships.csv",
    "patent_cpc": "clean_cpc.csv",
}


def _execute_schema(conn: duckdb.DuckDBPyConnection, schema_file: Path) -> None:
    sql = schema_file.read_text(encoding="utf-8")
    conn.execute(sql)


def _copy_csv(conn: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    # Use read_csv_auto then INSERT — it's the most robust way with DuckDB
    # when columns have mixed types across rows.
    conn.execute(
        f"""
        INSERT INTO {table}
        SELECT * FROM read_csv_auto('{path.as_posix()}', header=True, null_padding=true)
        """
    )
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"loaded {table}: {count:,} rows")
    return count


def run_load(settings: Settings) -> Path:
    db_path = settings.paths.warehouse_db
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    logger.info(f"opening DuckDB warehouse at {db_path}")
    with duckdb.connect(db_path.as_posix()) as conn:
        _execute_schema(conn, settings.paths.schema_file)
        for table, filename in TABLE_TO_FILE.items():
            csv_path = settings.paths.clean_dir / filename
            if not csv_path.exists():
                logger.warning(f"skip {table}: {csv_path} missing")
                continue
            _copy_csv(conn, table, csv_path)
    logger.info(f"warehouse ready: {db_path}")
    return db_path
