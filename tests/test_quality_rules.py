from datetime import datetime

from pyspark.sql import functions as F

from lakesense.quality.rules import Rule, apply_rules, summarize


def _rules():
    return [
        Rule.from_dict({"name": "id_nn", "type": "not_null", "column": "id"}),
        Rule.from_dict({"name": "qty_range", "type": "range", "column": "qty", "min": 1, "max": 10}),
        Rule.from_dict({"name": "status_ok", "type": "accepted_values", "column": "status",
                        "values": ["a", "b"]}),
        Rule.from_dict({"name": "email_ok", "type": "regex", "column": "email",
                        "pattern": "^[^@]+@[^@]+$", "severity": "warn"}),
        Rule.from_dict({"name": "ts_ok", "type": "not_future", "column": "ts"}),
        Rule.from_dict({"name": "fk_ok", "type": "referential", "column": "pid",
                        "ref_table": "dim", "ref_column": "pid"}),
    ]


def _df(spark):
    rows = [
        ("1", 5, "a", "x@y.com", datetime(2026, 1, 1), "P1"),   # clean
        (None, 5, "a", "x@y.com", datetime(2026, 1, 1), "P1"),  # null id
        ("3", 0, "a", "x@y.com", datetime(2026, 1, 1), "P1"),   # qty too low
        ("4", 5, "zzz", "bad-email", datetime(2026, 1, 1), "P1"),  # bad status + warn email
        ("5", 5, "b", "x@y.com", datetime(2099, 1, 1), "P1"),   # future
        ("6", 5, "b", "x@y.com", datetime(2026, 1, 1), "P404"),  # orphan FK
    ]
    return spark.createDataFrame(rows, "id string, qty int, status string, email string, ts timestamp, pid string")


def test_rules_flag_exactly_the_bad_rows(spark):
    dim = spark.createDataFrame([("P1",)], "pid string")
    out = apply_rules(_df(spark), _rules(), {"dim": dim})
    failed = {r["id"]: sorted(r["_dq_failed"]) for r in out.select("id", "_dq_failed").collect()}
    assert failed["1"] == []
    assert failed[None] == ["id_nn"]
    assert failed["3"] == ["qty_range"]
    assert failed["4"] == ["email_ok", "status_ok"]
    assert failed["5"] == ["ts_ok"]
    assert failed["6"] == ["fk_ok"]


def test_warn_rules_do_not_block(spark):
    dim = spark.createDataFrame([("P1",)], "pid string")
    out = apply_rules(_df(spark), _rules(), {"dim": dim})
    row4 = out.filter(F.col("id") == "4").first()
    assert "email_ok" in row4["_dq_failed"]
    assert "email_ok" not in row4["_dq_errors"]


def test_summarize_counts_and_samples(spark):
    dim = spark.createDataFrame([("P1",)], "pid string")
    rules = _rules()
    results = {r.rule: r for r in summarize("t", apply_rules(_df(spark), rules, {"dim": dim}), rules)}
    assert results["qty_range"].failed_rows == 1
    assert results["fk_ok"].sample_values == ["P404"]
    assert results["id_nn"].total_rows == 6
    assert 0 < results["status_ok"].pass_rate < 1
