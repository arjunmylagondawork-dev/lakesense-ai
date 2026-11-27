"""Spark session factory, tuned for local runs and CI."""

from __future__ import annotations

from pyspark.sql import SparkSession

from lakesense.config import Settings


def get_spark(settings: Settings) -> SparkSession:
    cfg = settings.section("spark")
    return (
        SparkSession.builder.appName(cfg.get("app_name", "lakesense-ai"))
        .master(cfg.get("master", "local[*]"))
        .config("spark.sql.shuffle.partitions", cfg.get("shuffle_partitions", 8))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        # full rebuilds: static overwrite replaces the whole table, so no stale partitions survive
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
