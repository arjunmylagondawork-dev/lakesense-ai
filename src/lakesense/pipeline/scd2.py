"""Slowly Changing Dimension Type 2 for customers.

`customer_history` keeps every version of a customer's tracked attributes:

    customer_sk | customer_id | segment | country | email | valid_from | valid_to | is_current

Rules
  * a new version is created only when a TRACKED attribute changes
    (re-sending identical data creates nothing - idempotent)
  * the previous version is closed: valid_to = next version's valid_from
  * several changes inside one batch are all kept, in time order
  * versions older than the current one are ignored (late, out-of-order data)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KEY = "customer_id"
TRACKED = ["segment", "country", "email"]
TS = "updated_at"
COLUMNS = ["customer_sk", KEY, *TRACKED, "valid_from", "valid_to", "is_current"]


def _attr_hash(cols: list[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def apply_scd2(current: DataFrame | None, incoming: DataFrame,
               key: str = KEY, tracked: list[str] = TRACKED, ts: str = TS) -> DataFrame:
    """Return the complete new history table (closed versions + re-derived open ones)."""
    inc = incoming.filter(F.col(key).isNotNull() & F.col(ts).isNotNull()).select(key, *tracked, ts)
    inc = inc.dropDuplicates([key, ts])

    closed = None
    if current is not None:
        closed = current.filter(~F.col("is_current")).select(*COLUMNS)
        cur = current.filter("is_current").select(key, *tracked, F.col("valid_from").alias(ts))
        cur_ts = cur.select(key, F.col(ts).alias("_cur_ts"))
        inc = (inc.join(cur_ts, key, "left")
               .filter(F.col("_cur_ts").isNull() | (F.col(ts) > F.col("_cur_ts")))
               .drop("_cur_ts"))
        base = cur.unionByName(inc)
    else:
        base = inc

    w = Window.partitionBy(key).orderBy(ts)
    changed = (base.withColumn("_h", _attr_hash(tracked))
               .withColumn("_prev", F.lag("_h").over(w))
               .filter(F.col("_prev").isNull() | (F.col("_h") != F.col("_prev")))
               .drop("_h", "_prev"))
    versions = (changed
                .withColumn("valid_to", F.lead(ts).over(Window.partitionBy(key).orderBy(ts)))
                .withColumn("is_current", F.col("valid_to").isNull())
                .withColumnRenamed(ts, "valid_from")
                .withColumn("customer_sk",
                            F.substring(F.sha2(F.concat_ws("|", F.col(key), F.col("valid_from").cast("string")), 256),
                                        1, 16))
                .select(*COLUMNS))
    return versions if closed is None else closed.unionByName(versions)
