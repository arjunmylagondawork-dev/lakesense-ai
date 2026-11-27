"""Serving layer: a DuckDB warehouse exposing gold + silver tables as SQL tables,
plus the dbt analytics models built on top of them.

DuckDB is the free, zero-setup stand-in for Redshift / Snowflake / Athena.
The text-to-SQL agent and the Streamlit app query this database read-only.

Release flow:  build into a temp file -> dbt build (models + tests + contracts)
               -> atomic swap.  A failing dbt test or contract blocks the release.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
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
    run_dbt(settings, tmp)
    tmp.replace(db_path)   # atomic swap: readers never see a half-built warehouse
    return db_path


class DbtBuildError(RuntimeError):
    pass


def run_dbt(settings: Settings, db_file: Path) -> bool:
    """`dbt build` (run models, then tests and contracts) against the warehouse being released.

    dbt runs in a child process: dbt-duckdb keeps its database connection open for the
    life of the process, which would block the atomic file swap (on Windows) and make a
    second publish in the same process reuse a stale database.
    """
    cfg = settings.section("dbt")
    if not cfg.get("enabled", True):
        return False
    if importlib.util.find_spec("dbt") is None:
        if cfg.get("required", False):
            raise DbtBuildError("dbt is required but not installed: pip install -e '.[dbt]'")
        log.warning("dbt not installed - skipping analytics models (pip install -e '.[dbt]')")
        return False
    from lakesense.config import PROJECT_ROOT

    project = Path(cfg.get("project_dir", "dbt"))
    project = project if project.is_absolute() else PROJECT_ROOT / project
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(project), "--profiles-dir", str(project),
           "--target-path", str(settings.paths.reports / "dbt_target"),
           "--log-path", str(settings.paths.reports / "dbt_logs")]
    env = {**os.environ, "LAKESENSE_DUCKDB_PATH": str(db_file)}
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=project)
    summary = next((ln for ln in reversed(proc.stdout.splitlines()) if "PASS=" in ln), "").strip()
    if proc.returncode != 0:
        failures = [ln.strip() for ln in proc.stdout.splitlines() if "FAIL" in ln or "ERROR" in ln][:10]
        raise DbtBuildError("dbt build failed - warehouse NOT published.\n" + "\n".join(failures))
    log.info("dbt build passed: %s", summary.split("Done. ")[-1] or "ok")
    return True


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
