from datetime import date

from lakesense.pipeline.gold import build_customer_rfm, build_daily_revenue
from lakesense.pipeline.silver import clean_orders

ORDER_COLS = ("order_line_id string, order_id string, order_ts string, customer_id string, "
              "product_id string, quantity string, unit_price string, status string, "
              "payment_method string, country string, _source_file string, _ingest_ts timestamp, "
              "_batch_id string")


def _bronze_orders(spark, rows):
    return spark.createDataFrame(rows, ORDER_COLS)


def test_clean_orders_parses_formats_and_bad_types(spark):
    rows = [
        ("L1", "O1", "2026-06-01 10:00:00", "C1", "P1", "2", "10.50", "Delivered", "UPI", "in", "f", None, "b"),
        ("L2", "O2", "01/06/2026 11:30", "C1", "P1", "two", "5", "placed", "card", "IN", "f", None, "b"),
        ("L3", "O3", "not a date", "", "P1", "1", "abc", "placed", "card", "IN", "f", None, "b"),
    ]
    out = {r["order_line_id"]: r for r in clean_orders(_bronze_orders(spark, rows)).collect()}
    assert out["L1"]["order_date"] == date(2026, 6, 1)
    assert out["L1"]["status"] == "delivered" and out["L1"]["country"] == "IN"
    assert float(out["L1"]["line_amount"]) == 21.0
    assert out["L2"]["order_date"] == date(2026, 6, 1)        # legacy format parsed
    assert out["L2"]["quantity"] is None                       # 'two' -> NULL, no crash
    assert out["L3"]["order_ts"] is None and out["L3"]["unit_price"] is None
    assert out["L3"]["customer_id"] is None                    # blank -> NULL


def test_clean_orders_deduplicates(spark):
    row = ("L1", "O1", "2026-06-01 10:00:00", "C1", "P1", "1", "10", "placed", "card", "IN", "f", None, "b")
    assert clean_orders(_bronze_orders(spark, [row, row])).count() == 1


def _silver_orders(spark):
    rows = [
        ("O1", "C1", date(2026, 6, 1), 1, 100.0, "delivered"),
        ("O2", "C2", date(2026, 6, 1), 2, 50.0, "cancelled"),   # excluded from revenue
        ("O3", "C1", date(2026, 6, 2), 1, 30.0, "shipped"),
    ]
    df = spark.createDataFrame(rows, "order_id string, customer_id string, order_date date, "
                                     "quantity int, line_amount double, status string")
    return df


def test_daily_revenue_excludes_cancelled(spark):
    out = {r["order_date"]: r for r in build_daily_revenue(_silver_orders(spark)).collect()}
    assert float(out[date(2026, 6, 1)]["revenue"]) == 100.0
    assert out[date(2026, 6, 1)]["orders"] == 1
    assert float(out[date(2026, 6, 2)]["revenue_7d_avg"]) == 65.0


def test_rfm_scores_and_segments(spark):
    customers = spark.createDataFrame([("C1", "A", "IN", "consumer")],
                                      "customer_id string, name string, country string, segment string")
    out = build_customer_rfm(_silver_orders(spark), customers).collect()
    assert len(out) == 1
    c1 = out[0]
    assert c1["frequency"] == 2 and float(c1["monetary"]) == 130.0 and c1["recency_days"] == 0
    assert 1 <= c1["r_score"] <= 5
    assert c1["rfm_segment"] in {"champion", "loyal", "at_risk", "new_or_promising", "hibernating"}
