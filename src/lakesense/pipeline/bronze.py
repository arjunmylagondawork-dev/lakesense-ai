"""Bronze layer: incremental, append-only landing of raw files (all strings) + lineage.

* Only files not seen before are read (tracked in the pipeline state), so a
  re-run is a no-op and new daily files are picked up automatically.
* Each new file is checked against its schema contract first: files missing a
  required column are rejected; new columns are kept (schema evolution) and
  reported as drift.
* Files are grouped by header before reading, so a file with an extra column
  can never shift values into the wrong columns.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.quality.contracts import DriftResult, check_file
from lakesense.state import PipelineState
from lakesense.storage import Lake

log = get_logger(__name__)

SOURCES = {
    "customers": ("json", "customers", "*.jsonl"),
    "products": ("csv", "products", "*.csv"),
    "orders": ("csv", "orders", "*.csv"),
}


def _read(spark: SparkSession, fmt: str, paths: list[str]) -> DataFrame:
    if fmt == "csv":
        # inferSchema=False -> everything is a string; typing happens in silver
        return spark.read.option("header", True).option("inferSchema", False).csv(paths)
    df = spark.read.json(paths, primitivesAsString=True)
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])


def add_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def read_new_files(spark: SparkSession, fmt: str, accepted: list[tuple[Path, DriftResult]]) -> DataFrame | None:
    """Read files grouped by identical header, then union by column name."""
    groups: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for path, drift in accepted:
        groups[tuple(drift.columns)].append(str(path))
    frames = [_read(spark, fmt, paths) for paths in groups.values()]
    if not frames:
        return None
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    return out


def run_bronze(spark: SparkSession, settings: Settings, full: bool = False) -> dict[str, int]:
    """Ingest new raw files. `full=True` resets state and rebuilds bronze from all files."""
    lake = Lake(spark, settings)
    state = PipelineState.reset(settings) if full else PipelineState.load(settings)
    contracts = settings.section("contracts")
    batch_id = uuid.uuid4().hex[:12]
    counts: dict[str, int] = {}
    drift_report: list[dict] = []
    n_files = 0

    for table, (fmt, subdir, glob) in SOURCES.items():
        src_dir = settings.paths.raw / subdir
        files = sorted(src_dir.glob(glob))
        new = [p for p in files if full or state.is_new(f"{subdir}/{p.name}", p)]
        accepted: list[tuple[Path, DriftResult]] = []
        for p in new:
            drift = check_file(table, p, fmt, contracts.get(table, {}))
            if drift.has_drift:
                drift_report.append(drift.to_dict())
            if drift.accepted:
                accepted.append((p, drift))
            else:
                log.warning("bronze.%s: REJECTED %s - missing required %s", table, p.name, drift.missing_required)
                state.record_file(f"{subdir}/{p.name}", p, batch_id, "rejected",
                                  reason=f"missing required columns {drift.missing_required}")

        df = read_new_files(spark, fmt, accepted)
        if df is None:
            counts[table] = 0
            log.info("bronze.%s: no new files", table)
            continue
        df = add_lineage(df, batch_id)
        lake.write(df, "bronze", table, mode="overwrite" if full else "append")
        counts[table] = lake.read("bronze", table).filter(F.col("_batch_id") == batch_id).count()
        for p, _ in accepted:
            state.record_file(f"{subdir}/{p.name}", p, batch_id, "ingested")
        n_files += len(accepted)
        log.info("bronze.%s <- %d rows from %d new file(s) (batch %s)", table, counts[table], len(accepted), batch_id)

    if n_files:
        state.add_batch(batch_id, n_files)
    state.save()
    _write_drift_report(settings, drift_report, reset=full)
    return counts


def _write_drift_report(settings: Settings, drift: list[dict], reset: bool) -> None:
    reports = settings.paths.reports
    reports.mkdir(parents=True, exist_ok=True)
    path = reports / "schema_drift.json"
    history = [] if reset or not path.exists() else json.loads(path.read_text())
    path.write_text(json.dumps(history + drift, indent=2), encoding="utf-8")
