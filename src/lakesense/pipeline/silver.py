"""Silver layer: typed, cleaned, de-duplicated, validated.

* ANSI-safe parsing (try_cast / try_to_timestamp) - bad values become NULL
  instead of crashing the job, and the DQ rules then catch them.
* Deduplication with window functions (latest record wins).
* Rows that fail an error-severity rule go to quarantine/, with the reasons.
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.quality.rules import Rule, RuleResult, apply_rules, summarize

log = get_logger(__name__)

LINEAGE = ["_source_file", "_ingest_ts", "_batch_id"]


def _try_cast(col: str, dtype: str):
    return F.expr(f"try_cast(trim(`{col}`) AS {dtype})")


def _parse_ts(col: str):
    """Accept the primary ISO format and a legacy dd/MM/yyyy HH:mm format."""
    c = F.trim(F.col(col))
    return F.coalesce(
        F.try_to_timestamp(c, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(c, F.lit("dd/MM/yyyy HH:mm")),
    )


def _blank_to_null(col: str):
    return F.when(F.trim(F.col(col)) == "", None).otherwise(F.trim(F.col(col)))


def dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_customers(df: DataFrame) -> DataFrame:
    df = df.select(
        _blank_to_null("customer_id").alias("customer_id"),
        F.initcap(F.trim("name")).alias("name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("country")).alias("country"),
        F.lower(F.trim("segment")).alias("segment"),
        _try_cast("signup_date", "DATE").alias("signup_date"),
        F.try_to_timestamp(F.col("updated_at"), F.lit("yyyy-MM-dd'T'HH:mm:ss")).alias("updated_at"),
        *LINEAGE,
    )
    return dedupe_latest(df, ["customer_id"], "updated_at")


def clean_products(df: DataFrame) -> DataFrame:
    df = df.select(
        _blank_to_null("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.trim("category").alias("category"),
        _try_cast("list_price", "DECIMAL(12,2)").alias("list_price"),
        *LINEAGE,
    )
    return dedupe_latest(df, ["product_id"], "_ingest_ts")


def clean_orders(df: DataFrame) -> DataFrame:
    df = df.select(
        _blank_to_null("order_line_id").alias("order_line_id"),
        _blank_to_null("order_id").alias("order_id"),
        _parse_ts("order_ts").alias("order_ts"),
        _blank_to_null("customer_id").alias("customer_id"),
        _blank_to_null("product_id").alias("product_id"),
        _try_cast("quantity", "INT").alias("quantity"),
        _try_cast("unit_price", "DECIMAL(12,2)").alias("unit_price"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
        F.upper(F.trim("country")).alias("country"),
        *LINEAGE,
    )
    df = df.withColumn("order_date", F.to_date("order_ts"))
    df = df.withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
    # exact duplicate lines from the source are removed; same id -> keep latest ingest
    return dedupe_latest(df, ["order_line_id"], "_ingest_ts")


CLEANERS = {"customers": clean_customers, "products": clean_products, "orders": clean_orders}
ORDER = ["customers", "products", "orders"]   # dimensions first: orders reference them


def run_silver(spark: SparkSession, settings: Settings) -> list[RuleResult]:
    rules_cfg = settings.section("quality")
    lake = settings.paths
    silver_dfs: dict[str, DataFrame] = {}
    all_results: list[RuleResult] = []

    for table in ORDER:
        bronze = spark.read.parquet(str(lake.layer("bronze") / table))
        cleaned = CLEANERS[table](bronze)
        rules = [Rule.from_dict(r) for r in rules_cfg.get(table, [])]
        checked = apply_rules(cleaned, rules, ref_dfs=silver_dfs).cache()

        all_results.extend(summarize(table, checked, rules))

        good = checked.filter(F.size("_dq_errors") == 0).drop("_dq_errors")
        bad = checked.filter(F.size("_dq_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())

        writer = good.write.mode("overwrite")
        if table == "orders":
            writer = writer.partitionBy("order_date")
        writer.parquet(str(lake.layer("silver") / table))
        bad.write.mode("overwrite").parquet(str(lake.layer("quarantine") / table))

        silver_dfs[table] = spark.read.parquet(str(lake.layer("silver") / table))
        log.info("silver.%s: %d valid, %d quarantined", table, silver_dfs[table].count(), bad.count())
        checked.unpersist()

    lake.reports.mkdir(parents=True, exist_ok=True)
    with open(lake.reports / "dq_results.json", "w", encoding="utf-8") as fh:
        json.dump([r.to_dict() for r in all_results], fh, indent=2)
    return all_results
