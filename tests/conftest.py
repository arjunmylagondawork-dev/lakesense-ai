from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lakesense.config import DEFAULT_CONFIG, load_settings

os.environ.setdefault("LAKESENSE_LLM_PROVIDER", "mock")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]").appName("lakesense-tests")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def small_settings(tmp_path_factory) -> object:
    """Settings pointing at a temp dir with a small, fast dataset."""
    root: Path = tmp_path_factory.mktemp("lakesense")
    cfg = yaml.safe_load(DEFAULT_CONFIG.read_text())
    cfg["generator"].update(n_customers=200, n_products=40, days=40, avg_lines_per_day=60,
                            anomalies=[{"day_offset": 30, "kind": "spike", "factor": 5.0}])
    cfg["spark"]["master"] = "local[2]"
    cfg["spark"]["shuffle_partitions"] = 2
    path = root / "pipeline.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return load_settings(path, root=root)
