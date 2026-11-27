from datetime import datetime

import pytest
from pyspark.sql import functions as F

from conftest import TABLE_FORMAT, delta_only
from lakesense.pipeline.scd2 import apply_scd2
from lakesense.quality.contracts import check_file
from lakesense.storage import Lake, active_parquet_files

ROWS = "id string, val string, ts timestamp"


def test_merge_inserts_updates_and_keeps_latest(spark, fresh_settings):
    lake = Lake(spark, fresh_settings)
    first = spark.createDataFrame([("a", "1", datetime(2026, 1, 1)), ("b", "1", datetime(2026, 1, 1))], ROWS)
    lake.merge(first, "silver", "t", ["id"], "ts")
    second = spark.createDataFrame(
        [("a", "2", datetime(2026, 1, 2)),     # newer -> update
         ("b", "0", datetime(2025, 1, 1)),     # older -> ignored
         ("c", "1", datetime(2026, 1, 2))],    # new key -> insert
        ROWS)
    stats = lake.merge(second, "silver", "t", ["id"], "ts")
    out = {r["id"]: r["val"] for r in lake.read("silver", "t").collect()}
    assert out == {"a": "2", "b": "1", "c": "1"}
    assert stats["inserted"] == 1


def test_replace_can_overwrite_a_table_from_itself(spark, fresh_settings):
    lake = Lake(spark, fresh_settings)
    lake.write(spark.createDataFrame([("a", "1", datetime(2026, 1, 1))], ROWS), "silver", "t")
    lake.replace(lake.read("silver", "t").withColumn("val", F.lit("x")), "silver", "t")
    assert [r["val"] for r in lake.read("silver", "t").collect()] == ["x"]


@pytest.mark.skipif(TABLE_FORMAT == "delta", reason="parquet-only behaviour")
def test_time_travel_needs_delta(spark, fresh_settings):
    lake = Lake(spark, fresh_settings)
    lake.write(spark.createDataFrame([("a", "1", datetime(2026, 1, 1))], ROWS), "silver", "t")
    with pytest.raises(ValueError):
        lake.read("silver", "t", version=0)


@delta_only
def test_delta_history_and_time_travel(spark, fresh_settings):
    lake = Lake(spark, fresh_settings)
    lake.merge(spark.createDataFrame([("a", "1", datetime(2026, 1, 1))], ROWS), "silver", "t", ["id"], "ts")
    lake.merge(spark.createDataFrame([("a", "2", datetime(2026, 1, 2))], ROWS), "silver", "t", ["id"], "ts")
    ops = [r["operation"] for r in lake.history("silver", "t").collect()]
    assert "MERGE" in ops
    assert lake.read("silver", "t", version=0).first()["val"] == "1"
    assert lake.read("silver", "t").first()["val"] == "2"
    # publish must only see live files, not ones removed by the MERGE
    files = active_parquet_files(lake.path("silver", "t"))
    assert len(files) >= 1 and all(f.endswith(".parquet") for f in files)


def test_active_files_follow_the_delta_log(tmp_path):
    """Delta tables keep removed files on disk; only the log knows which are live."""
    import pyarrow as pa
    from deltalake import write_deltalake

    path = tmp_path / "t"
    write_deltalake(str(path), pa.table({"x": [1, 2]}))
    write_deltalake(str(path), pa.table({"x": [3]}), mode="overwrite")
    on_disk = list(path.glob("*.parquet"))
    live = active_parquet_files(path)
    assert len(on_disk) == 2 and len(live) == 1


CUST = "customer_id string, segment string, country string, email string, updated_at timestamp"


def _hist(spark, rows):
    return spark.createDataFrame(rows, CUST)


def test_scd2_builds_versions_and_skips_unchanged(spark):
    inc = _hist(spark, [
        ("C1", "consumer", "IN", "a@x.com", datetime(2026, 1, 1)),
        ("C1", "consumer", "IN", "a@x.com", datetime(2026, 2, 1)),   # no change -> no version
        ("C1", "smb", "IN", "a@x.com", datetime(2026, 3, 1)),        # change -> new version
        ("C2", "consumer", "AE", "b@x.com", datetime(2026, 1, 5)),
    ])
    h = {(r["customer_id"], r["segment"]): r for r in apply_scd2(None, inc).collect()}
    assert len(h) == 3
    old, new = h[("C1", "consumer")], h[("C1", "smb")]
    assert old["valid_to"] == new["valid_from"] == datetime(2026, 3, 1)
    assert not old["is_current"] and new["is_current"] and h[("C2", "consumer")]["is_current"]


def test_scd2_incremental_closes_current_and_is_idempotent(spark):
    first = apply_scd2(None, _hist(spark, [("C1", "consumer", "IN", "a@x.com", datetime(2026, 1, 1))])).cache()
    update = _hist(spark, [("C1", "consumer", "GB", "a@x.com", datetime(2026, 4, 1))])
    second = apply_scd2(first, update).cache()
    rows = sorted(second.collect(), key=lambda r: r["valid_from"])
    assert [r["country"] for r in rows] == ["IN", "GB"]
    assert [r["is_current"] for r in rows] == [False, True]
    # re-sending the same update (or an older one) changes nothing
    again = apply_scd2(second, update.unionByName(
        _hist(spark, [("C1", "smb", "IN", "a@x.com", datetime(2025, 1, 1))])))
    assert again.count() == 2 and again.filter("is_current").count() == 1


def test_contract_check_csv_and_json(tmp_path):
    contract = {"required": ["id", "amount"], "optional": ["note"]}
    good = tmp_path / "a.csv"
    good.write_text("id,amount,promo\n1,2,X\n")
    r = check_file("orders", good, "csv", contract)
    assert r.accepted and r.new_columns == ["promo"] and r.missing_optional == ["note"]
    bad = tmp_path / "b.jsonl"
    bad.write_text('{"id": 1}\n{"id": 2, "note": "x"}\n')
    r = check_file("orders", bad, "json", contract)
    assert not r.accepted and r.missing_required == ["amount"]
