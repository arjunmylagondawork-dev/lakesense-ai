"""Silver layer: typed, cleaned, de-duplicated, validated - incrementally.

* Only bronze batches not yet processed are read (see lakesense.state).
* ANSI-safe parsing (try_cast / try_to_timestamp) - bad values become NULL
  instead of crashing the job, and the DQ rules then catch them.
* Deduplication with window functions (latest record wins).
* Valid rows are UPSERTED into silver with MERGE (late corrections update the
  existing row); rows failing an error rule are appended to quarantine/.
* Customer attribute changes are kept as SCD Type 2 history (customer_history).
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.pipeline.scd2 import apply_scd2
from lakesense.quality.rules import Rule, RuleResult, apply_rules, summarize
from lakesense.state import PipelineState
from lakesense.storage import Lake

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
    """Keep the latest row per key. Rows with a NULL key are NOT collapsed into one
    (they would share a window partition); they pass through so the not_null rule
    quarantines every one of them with a reason."""
    has_key = F.lit(True)
    for k in keys:
        has_key = has_key & F.col(k).isNotNull()
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    keyed = df.filter(has_key).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return keyed.unionByName(df.filter(~has_key))


def clean_customers(df: DataFrame, dedupe: bool = True) -> DataFrame:
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
    return dedupe_latest(df, ["customer_id"], "updated_at") if dedupe else df


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
# table -> (merge keys, "newer wins" column, partition columns)
MERGE_SPEC = {
    "customers": (["customer_id"], "updated_at", None),
    "products": (["product_id"], "_ingest_ts", None),
    "orders": (["order_line_id"], "_ingest_ts", ["order_date"]),
}


def update_customer_history(lake: Lake, bronze_customers: DataFrame, rules: list[Rule]) -> int:
    """SCD2: every valid customer version in the batch (before dedupe) feeds the history."""
    versions = apply_rules(clean_customers(bronze_customers, dedupe=False), rules)
    versions = versions.filter(F.size("_dq_errors") == 0)
    current = lake.read("silver", "customer_history") if lake.exists("silver", "customer_history") else None
    history = apply_scd2(current, versions)
    lake.replace(history, "silver", "customer_history")
    return lake.read("silver", "customer_history").count()


def run_silver(spark: SparkSession, settings: Settings, full: bool = False) -> list[RuleResult]:
    """Process pending bronze batches. `full=True` rebuilds silver from all of bronze."""
    lake = Lake(spark, settings)
    state = PipelineState.load(settings)
    pending = state.pending_batches()
    if not pending and not full:
        log.info("silver: no new bronze batches - nothing to do")
        return []

    rules_cfg = settings.section("quality")
    all_results: list[RuleResult] = []
    for table in ORDER:
        bronze = lake.read("bronze", table)
        if not full:
            bronze = bronze.filter(F.col("_batch_id").isin(pending))
        rules = [Rule.from_dict(r) for r in rules_cfg.get(table, [])]
        # referential checks run against the full, already-updated silver dimensions
        refs = {t: lake.read("silver", t) for t in ORDER if t != table and lake.exists("silver", t)}
        checked = apply_rules(CLEANERS[table](bronze), rules, ref_dfs=refs).cache()
        all_results.extend(summarize(table, checked, rules))

        good = checked.filter(F.size("_dq_errors") == 0).drop("_dq_errors", "_dq_failed")
        bad = checked.filter(F.size("_dq_errors") > 0).withColumn("_quarantined_at", F.current_timestamp())
        keys, order_col, parts = MERGE_SPEC[table]
        if full:
            lake.write(good, "silver", table, "overwrite", parts)
            stats = {"inserted": lake.read("silver", table).count(), "updated": 0, "mode": "full rebuild"}
        else:
            stats = lake.merge(good, "silver", table, keys, order_col, parts)
        n_bad = bad.count()
        if n_bad or full:
            lake.write(bad, "quarantine", table, "overwrite" if full else "append")
        log.info("silver.%s: %d inserted, %d updated (%s), %d quarantined",
                 table, stats["inserted"], stats["updated"], stats["mode"], n_bad)

        if table == "customers":
            n_hist = _rebuild_or_update_history(lake, bronze, rules, full)
            log.info("silver.customer_history: %d versions", n_hist)
        checked.unpersist()

    state.mark_silver_done(pending)
    state.save()
    _write_dq_results(settings, all_results)
    return all_results


def _rebuild_or_update_history(lake: Lake, bronze: DataFrame, rules: list[Rule], full: bool) -> int:
    if full:
        versions = apply_rules(clean_customers(bronze, dedupe=False), rules).filter(F.size("_dq_errors") == 0)
        lake.write(apply_scd2(None, versions), "silver", "customer_history", "overwrite")
        return lake.read("silver", "customer_history").count()
    return update_customer_history(lake, bronze, rules)


def _write_dq_results(settings: Settings, results: list[RuleResult]) -> None:
    reports = settings.paths.reports
    reports.mkdir(parents=True, exist_ok=True)
    with open(reports / "dq_results.json", "w", encoding="utf-8") as fh:
        json.dump([r.to_dict() for r in results], fh, indent=2)
