from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lakesense.config import DEFAULT_CONFIG, load_settings

os.environ.setdefault("LAKESENSE_LLM_PROVIDER", "mock")
# Tests default to plain Parquet (no jar download). CI also runs them with
# LAKESENSE_TABLE_FORMAT=delta to exercise real Delta Lake MERGE / time travel.
os.environ.setdefault("LAKESENSE_TABLE_FORMAT", "parquet")
TABLE_FORMAT = os.environ["LAKESENSE_TABLE_FORMAT"]

delta_only = pytest.mark.skipif(TABLE_FORMAT != "delta", reason="needs LAKESENSE_TABLE_FORMAT=delta")


@pytest.fixture(scope="session")
def spark():
    from lakesense.spark_session import build_spark

    session = build_spark("lakesense-tests", "local[2]", 2, fmt=TABLE_FORMAT)
    yield session
    session.stop()


def _settings(root: Path, **generator):
    cfg = yaml.safe_load(DEFAULT_CONFIG.read_text())
    defaults = dict(n_customers=200, n_products=40, days=40, avg_lines_per_day=60,
                    anomalies=[{"day_offset": 30, "kind": "spike", "factor": 5.0}])
    cfg["generator"].update({**defaults, **generator})
    cfg["spark"]["master"] = "local[2]"
    cfg["spark"]["shuffle_partitions"] = 2
    path = root / "pipeline.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return load_settings(path, root=root)


@pytest.fixture(scope="session")
def small_settings(tmp_path_factory):
    """Settings pointing at a temp dir with a small, fast dataset."""
    return _settings(tmp_path_factory.mktemp("lakesense"))


@pytest.fixture()
def fresh_settings(tmp_path):
    """Isolated settings for tests that mutate lake state."""
    return _settings(tmp_path, days=10)
