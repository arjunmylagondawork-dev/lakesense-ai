"""Full pipeline on a small synthetic dataset: generate -> bronze -> silver -> gold -> AI."""

import json

from lakesense.ai import anomaly, dq_explainer
from lakesense.ai.text_to_sql import TextToSQLAgent
from lakesense.generator.generate_data import generate
from lakesense.pipeline.bronze import run_bronze
from lakesense.pipeline.gold import run_gold
from lakesense.pipeline.silver import run_silver
from lakesense.serving.warehouse import publish


def test_pipeline_end_to_end(spark, small_settings):
    s = small_settings
    generate(s)
    bronze = run_bronze(spark, s)
    assert bronze["orders"] > 0

    results = run_silver(spark, s)
    silver = spark.read.parquet(str(s.paths.layer("silver") / "orders")).count()
    quarantine = spark.read.parquet(str(s.paths.layer("quarantine") / "orders")).count()
    assert silver > 0 and quarantine > 0, "generator injects defects, so quarantine must not be empty"
    assert silver + quarantine <= bronze["orders"]         # duplicates removed
    assert any(r.failed_rows for r in results)

    # quarantine rows always carry the reason
    reasons = spark.read.parquet(str(s.paths.layer("quarantine") / "orders")).select("_dq_errors").collect()
    assert all(len(r[0]) > 0 for r in reasons)

    gold = run_gold(spark, s)
    assert gold["daily_revenue"] == s.section("generator")["days"]

    publish(s)
    report = dq_explainer.explain(s).read_text()
    assert "Executive summary" in report

    anomaly.run(s)
    found = json.loads((s.paths.reports / "anomalies.json").read_text())
    assert any(a["date"] == "2026-07-01" for a in found), "injected spike on day 30 must be detected"

    ans = TextToSQLAgent(s).ask("top 3 customers by spend")
    assert ans.ok and len(ans.data) == 3


def test_failing_dbt_test_blocks_the_warehouse_release(spark, fresh_settings, tmp_path):
    """Release gate: if any dbt test fails, the previous warehouse stays in place."""
    import shutil
    from dataclasses import replace

    import pytest

    from lakesense.config import PROJECT_ROOT
    from lakesense.serving.warehouse import DbtBuildError

    pytest.importorskip("dbt.cli.main")
    s = fresh_settings
    generate(s)
    run_bronze(spark, s)
    run_silver(spark, s)
    run_gold(spark, s)
    publish(s)                                   # good release
    before = s.paths.warehouse.stat().st_mtime_ns

    broken = tmp_path / "dbt_broken"
    shutil.copytree(PROJECT_ROOT / "dbt", broken, ignore=shutil.ignore_patterns("target", "logs"))
    (broken / "tests" / "always_fails.sql").write_text("select 1 as failure")
    raw = {**s.raw, "dbt": {**s.raw.get("dbt", {}), "project_dir": str(broken), "required": True}}
    with pytest.raises(DbtBuildError):
        publish(replace(s, raw=raw))
    assert s.paths.warehouse.stat().st_mtime_ns == before, "a failed build must not replace the warehouse"
