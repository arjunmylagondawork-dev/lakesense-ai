"""Gold layer: business-ready marts built from silver.

  daily_revenue        - one row per day: orders, customers, revenue, AOV, 7-day moving avg
  customer_rfm         - Recency / Frequency / Monetary scores + segment label
  product_performance  - revenue & units per product with in-category ranking
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.storage import Lake

log = get_logger(__name__)

REVENUE_STATUSES = ["placed", "shipped", "delivered"]   # cancelled/returned excluded


def revenue_lines(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("status").isin(REVENUE_STATUSES))


def build_daily_revenue(orders: DataFrame) -> DataFrame:
    daily = (
        revenue_lines(orders)
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
            F.sum("quantity").alias("units"),
            F.sum("line_amount").cast("decimal(16,2)").alias("revenue"),
        )
        .withColumn("avg_order_value", F.round(F.col("revenue") / F.col("orders"), 2))
    )
    w7 = Window.orderBy("order_date").rowsBetween(-6, 0)
    return (
        daily.withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(w7), 2))
        .withColumn("day_of_week", F.date_format("order_date", "E"))
        .orderBy("order_date")
    )


def build_customer_rfm(orders: DataFrame, customers: DataFrame) -> DataFrame:
    lines = revenue_lines(orders)
    as_of = lines.agg(F.max("order_date").alias("d")).first()["d"]
    rfm = lines.groupBy("customer_id").agg(
        F.datediff(F.lit(as_of), F.max("order_date")).alias("recency_days"),
        F.countDistinct("order_id").alias("frequency"),
        F.sum("line_amount").cast("decimal(16,2)").alias("monetary"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    # quintile scores 1..5 (5 = best); recency is "lower is better"
    rfm = (
        rfm.withColumn("r_score", 6 - F.ntile(5).over(Window.orderBy("recency_days")))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
    )
    segment = (
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "champion")
        .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), "loyal")
        .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), "at_risk")
        .when(F.col("r_score") >= 4, "new_or_promising")
        .otherwise("hibernating")
    )
    return (
        rfm.withColumn("rfm_segment", segment)
        .join(customers.select("customer_id", "name", "country", "segment"), "customer_id", "left")
    )


def build_product_performance(orders: DataFrame, products: DataFrame) -> DataFrame:
    perf = (
        revenue_lines(orders)
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("line_amount").cast("decimal(16,2)").alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
        )
        .join(products.select("product_id", "product_name", "category", "list_price"), "product_id")
    )
    w_cat = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    w_all = Window.partitionBy("category")
    return (
        perf.withColumn("rank_in_category", F.dense_rank().over(w_cat))
        .withColumn("category_revenue_share",
                    F.round(F.col("revenue") / F.sum("revenue").over(w_all), 4))
    )


def run_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Gold marts are small aggregates, so they are rebuilt from the full silver tables
    each run (cheap, and always consistent with silver after MERGEs)."""
    lake = Lake(spark, settings)
    orders = lake.read("silver", "orders")
    customers = lake.read("silver", "customers")
    products = lake.read("silver", "products")

    marts = {
        "daily_revenue": build_daily_revenue(orders),
        "customer_rfm": build_customer_rfm(orders, customers),
        "product_performance": build_product_performance(orders, products),
    }
    counts = {}
    for name, df in marts.items():
        lake.write(df, "gold", name, "overwrite")
        counts[name] = lake.read("gold", name).count()
        log.info("gold.%s -> %d rows", name, counts[name])
    return counts
