"""Table storage abstraction: Delta Lake (default) or plain Parquet.

Every layer reads and writes through `Lake`, so the pipeline code is identical
for both formats:

    format = delta    ACID tables, MERGE INTO upserts, schema evolution,
                      time travel (`lakesense history`), OPTIMIZE/VACUUM
    format = parquet  zero-dependency fallback; MERGE is emulated with
                      union + dedupe + atomic directory swap

Choose with `storage.format` in config/pipeline.yaml or LAKESENSE_TABLE_FORMAT.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger

log = get_logger(__name__)
FORMATS = ("delta", "parquet")


def table_format(settings: Settings) -> str:
    fmt = (os.getenv("LAKESENSE_TABLE_FORMAT") or settings.section("storage").get("format", "delta")).lower()
    if fmt not in FORMATS:
        raise ValueError(f"Unknown table format '{fmt}'. Choose from {FORMATS}")
    return fmt


def table_path(settings: Settings, layer: str, name: str) -> Path:
    return settings.paths.layer(layer) / name


def is_delta_table(path: Path) -> bool:
    return (path / "_delta_log").is_dir()


def active_parquet_files(path: Path) -> list[str]:
    """Data files that make up the *current* version of a table.

    For Delta, a directory listing would also return files that were logically
    removed by later MERGE/OVERWRITE commits, so we ask the Delta log (via the
    `deltalake` package) for the live file list instead.
    """
    if is_delta_table(path):
        from deltalake import DeltaTable  # optional dependency, installed with the delta format

        return [u.removeprefix("file://") for u in DeltaTable(str(path)).file_uris()]
    return sorted(str(p) for p in path.rglob("*.parquet"))


class Lake:
    def __init__(self, spark: SparkSession, settings: Settings):
        self.spark = spark
        self.settings = settings
        self.format = table_format(settings)

    # -- basics -----------------------------------------------------------------
    def path(self, layer: str, name: str) -> Path:
        return table_path(self.settings, layer, name)

    def exists(self, layer: str, name: str) -> bool:
        p = self.path(layer, name)
        return is_delta_table(p) if self.format == "delta" else p.exists() and any(p.rglob("*.parquet"))

    def read(self, layer: str, name: str, version: int | None = None) -> DataFrame:
        p = str(self.path(layer, name))
        if self.format == "delta":
            reader = self.spark.read.format("delta")
            if version is not None:
                reader = reader.option("versionAsOf", version)   # time travel
            return reader.load(p)
        if version is not None:
            raise ValueError("Time travel needs the delta table format")
        return self.spark.read.option("mergeSchema", True).parquet(p)

    def write(self, df: DataFrame, layer: str, name: str, mode: str = "overwrite",
              partition_by: list[str] | None = None) -> None:
        p = str(self.path(layer, name))
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if self.format == "delta":
            # append: new source columns are added (schema evolution);
            # overwrite: the table schema is replaced
            opt = "mergeSchema" if mode == "append" else "overwriteSchema"
            writer.format("delta").option(opt, "true").save(p)
        else:
            writer.parquet(p)

    def replace(self, df: DataFrame, layer: str, name: str, partition_by: list[str] | None = None) -> None:
        """Overwrite a table with a DataFrame that may be derived from that same table."""
        if self.format == "delta" or not self.exists(layer, name):
            # Delta reads a consistent snapshot, so overwriting a table that the
            # DataFrame was read from is safe.
            self.write(df, layer, name, "overwrite", partition_by)
            return
        final = self.path(layer, name)
        tmp = final.with_name(f"{final.name}__tmp_{uuid.uuid4().hex[:8]}")
        writer = df.write.mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(str(tmp))                       # materialise first ...
        old = final.with_name(f"{final.name}__old")
        final.rename(old)                              # ... then swap directories
        tmp.rename(final)
        shutil.rmtree(old, ignore_errors=True)

    # -- upsert -------------------------------------------------------------------
    def merge(self, df: DataFrame, layer: str, name: str, keys: list[str], order_col: str,
              partition_by: list[str] | None = None) -> dict[str, Any]:
        """Upsert `df` into a table: insert new keys, update a key only when the
        incoming row is at least as new (`order_col`)."""
        if not self.exists(layer, name):
            self.write(df, layer, name, "overwrite", partition_by)
            return {"inserted": df.count(), "updated": 0, "mode": "initial load"}

        if self.format == "delta":
            from delta.tables import DeltaTable

            target = DeltaTable.forPath(self.spark, str(self.path(layer, name)))
            cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
            (
                target.alias("t")
                .merge(df.alias("s"), cond)
                .whenMatchedUpdateAll(condition=f"s.`{order_col}` >= t.`{order_col}`")
                .whenNotMatchedInsertAll()
                .execute()
            )
            m = target.history(1).select("operationMetrics").first()[0] or {}
            return {"inserted": int(m.get("numTargetRowsInserted", 0)),
                    "updated": int(m.get("numTargetRowsUpdated", 0)), "mode": "delta MERGE"}

        # parquet: emulate MERGE with union + latest-wins dedupe, then swap atomically
        current = self.read(layer, name)
        existing_keys = current.select(*keys).distinct()
        n_new = df.join(existing_keys, keys, "left_anti").count()
        combined = current.unionByName(df, allowMissingColumns=True)
        w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
        merged = combined.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        self.replace(merged, layer, name, partition_by)
        return {"inserted": n_new, "updated": df.count() - n_new, "mode": "parquet merge (emulated)"}

    # -- delta-only operations ---------------------------------------------------
    def history(self, layer: str, name: str, limit: int = 20) -> DataFrame:
        self._require_delta("history")
        from delta.tables import DeltaTable

        return (DeltaTable.forPath(self.spark, str(self.path(layer, name))).history(limit)
                .select("version", "timestamp", "operation", "operationMetrics"))

    def optimize(self, layer: str, name: str, zorder_by: list[str] | None = None) -> None:
        self._require_delta("optimize")
        from delta.tables import DeltaTable

        opt = DeltaTable.forPath(self.spark, str(self.path(layer, name))).optimize()
        opt.executeZOrderBy(*zorder_by) if zorder_by else opt.executeCompaction()

    def vacuum(self, layer: str, name: str, retain_hours: int = 168) -> None:
        self._require_delta("vacuum")
        from delta.tables import DeltaTable

        DeltaTable.forPath(self.spark, str(self.path(layer, name))).vacuum(retain_hours)

    def _require_delta(self, op: str) -> None:
        if self.format != "delta":
            raise RuntimeError(f"`{op}` needs storage.format = delta (current: {self.format})")
