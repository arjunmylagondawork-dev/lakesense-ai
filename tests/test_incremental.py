"""Incremental loads: idempotent re-runs, MERGE of late corrections, drift handling, SCD2."""

from pyspark.sql import functions as F

from lakesense.generator.generate_data import append_days, generate
from lakesense.pipeline.bronze import run_bronze
from lakesense.pipeline.silver import run_silver
from lakesense.state import PipelineState
from lakesense.storage import Lake


def test_incremental_pipeline(spark, fresh_settings):
    s = fresh_settings
    lake = Lake(spark, s)
    generate(s)
    run_bronze(spark, s)
    run_silver(spark, s)
    orders_v1 = lake.read("silver", "orders").count()
    hist_v1 = lake.read("silver", "customer_history").count()

    # 1. re-running without new files is a no-op
    assert run_bronze(spark, s) == {"customers": 0, "products": 0, "orders": 0}
    assert run_silver(spark, s) == []
    assert lake.read("silver", "orders").count() == orders_v1

    # 2. new days with late corrections + a new column
    append_days(s, days=2, drift="new-column")
    counts = run_bronze(spark, s)
    assert counts["orders"] > 0 and counts["products"] == 0
    assert "promo_code" in lake.read("bronze", "orders").columns          # schema evolution
    run_silver(spark, s)
    orders_v2 = lake.read("silver", "orders")
    n_orders_v2 = orders_v2.count()
    assert n_orders_v2 > orders_v1
    assert orders_v2.groupBy("order_line_id").count().filter("count > 1").count() == 0   # MERGE, no dups
    # corrections re-send order lines from the original 10 days (up to 2026-06-10)
    corrected = orders_v2.filter(F.col("order_date") <= F.lit("2026-06-10")).filter(
        F.col("_batch_id") == PipelineState.load(s).batches[-1]["batch_id"])
    assert corrected.count() > 0, "late corrections to earlier days must update existing rows"
    assert lake.read("silver", "customer_history").count() > hist_v1      # SCD2 grew

    # 3. a file missing a required column is rejected, the rest of the batch still loads
    append_days(s, days=1, drift="missing-column")
    run_bronze(spark, s)
    rejected = [k for k, v in PipelineState.load(s).files.items() if v["status"] == "rejected"]
    assert rejected and rejected[0].endswith("_partner.csv")
    run_silver(spark, s)
    assert lake.read("silver", "orders").count() > n_orders_v2

    # history is consistent: exactly one current version per customer
    hist = lake.read("silver", "customer_history")
    assert hist.filter("is_current").count() == hist.select("customer_id").distinct().count()
