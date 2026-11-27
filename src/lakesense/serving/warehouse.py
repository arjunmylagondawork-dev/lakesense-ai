"""Serving layer: a DuckDB warehouse exposing gold + silver parquet as SQL tables.

DuckDB is the free, zero-setup stand-in for Redshift / Snowflake / Athena.
The text-to-SQL agent and the Streamlit app query this database read-only.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.storage import active_parquet_files

log = get_logger(__name__)

GOLD = ["daily_revenue", "customer_rfm", "product_performance"]
SILVER = ["orders", "customers", "products", "customer_history"]


def publish(settings: Settings) -> Path:
    db_path = settings.paths.warehouse
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = db_path.with_suffix(".tmp.duckdb")
    tmp.unlink(missing_ok=True)
    with duckdb.connect(str(tmp)) as con:
        for layer, tables in (("gold", GOLD), ("silver", SILVER)):
            for t in tables:
                files = active_parquet_files(settings.paths.layer(layer) / t)
                if not files:
                    log.warning("warehouse.%s: no data files, skipped", t)
                    continue
                file_list = "[" + ", ".join(f"'{f}'" for f in files) + "]"
                lineage = {"_source_file", "_ingest_ts", "_batch_id"}
                cols = [r[0] for r in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet({file_list}, hive_partitioning = true, "
                    f"union_by_name = true)").fetchall()]
                drop = [c for c in cols if c in lineage]
                exclude = f"EXCLUDE ({', '.join(drop)})" if drop else ""
                con.execute(
                    f"CREATE TABLE {t} AS SELECT * {exclude} "
                    f"FROM read_parquet({file_list}, hive_partitioning = true, union_by_name = true)"
                )
                n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                log.info("warehouse.%s <- %s rows", t, n)
    tmp.replace(db_path)   # atomic swap: readers never see a half-built warehouse
    return db_path


# Consumers get a read-only connection with file-system / network access disabled,
# so even SQL that slipped past the guardrails cannot read files or write anything.
# All consumers must use this same config (DuckDB refuses mixed configs per process).
READONLY_CONFIG = {"enable_external_access": False}


def connect_readonly(settings: Settings) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(settings.paths.warehouse), read_only=True, config=dict(READONLY_CONFIG))


def schema_description(con: duckdb.DuckDBPyConnection, tables: list[str]) -> str:
    """Compact schema text used as LLM context for text-to-SQL."""
    lines = []
    for t in tables:
        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position", [t]
        ).fetchall()
        if cols:
            lines.append(f"{t}(" + ", ".join(f"{c} {d}" for c, d in cols) + ")")
    return "\n".join(lines)
