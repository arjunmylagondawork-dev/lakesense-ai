"""Bronze layer: land raw files as-is (all strings) + lineage metadata.

Schema-on-read: nothing is cast or dropped here, so bronze is a faithful,
replayable copy of what the source systems sent.
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger

log = get_logger(__name__)

SOURCES = {
    "customers": ("json", "customers", "*.jsonl"),
    "products": ("csv", "products", "*.csv"),
    "orders": ("csv", "orders", "*.csv"),
}


def _read(spark: SparkSession, fmt: str, path: str, glob: str) -> DataFrame:
    reader = spark.read.option("pathGlobFilter", glob)
    if fmt == "csv":
        # inferSchema=False -> everything is a string; typing happens in silver
        return reader.option("header", True).option("inferSchema", False).csv(path)
    df = reader.json(path, primitivesAsString=True)
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])


def add_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
    )


def run_bronze(spark: SparkSession, settings: Settings, batch_id: str | None = None) -> dict[str, int]:
    batch_id = batch_id or uuid.uuid4().hex[:12]
    counts = {}
    for table, (fmt, subdir, glob) in SOURCES.items():
        src = str(settings.paths.raw / subdir)
        df = add_lineage(_read(spark, fmt, src, glob), batch_id)
        target = str(settings.paths.layer("bronze") / table)
        df.write.mode("overwrite").parquet(target)
        counts[table] = spark.read.parquet(target).count()
        log.info("bronze.%s <- %s rows (batch %s)", table, counts[table], batch_id)
    return counts
