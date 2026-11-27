"""Spark session factory, tuned for local runs and CI.

With the `delta` table format the Delta Lake SQL extension and catalog are
enabled, and the matching delta-spark jars are resolved from Maven the first
time Spark starts (cached under ~/.ivy2 afterwards).
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from lakesense.config import Settings
from lakesense.storage import table_format


def build_spark(app_name: str = "lakesense-ai", master: str = "local[*]", shuffle_partitions: int = 8,
                fmt: str = "parquet", ui: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        # full rebuilds: static overwrite replaces the whole table, so no stale partitions survive
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", str(ui).lower())
    )
    if fmt == "delta":
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # let MERGE add columns that appear in new source data (schema evolution)
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )
        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()


def get_spark(settings: Settings) -> SparkSession:
    cfg = settings.section("spark")
    return build_spark(
        app_name=cfg.get("app_name", "lakesense-ai"),
        master=cfg.get("master", "local[*]"),
        shuffle_partitions=cfg.get("shuffle_partitions", 8),
        fmt=table_format(settings),
        ui=cfg.get("ui", False),
    )
