"""Airflow DAG for LakeSense AI.

land -> bronze (incremental) -> silver (MERGE + SCD2) -> gold -> publish
     -> [dq_report, anomalies, drift_report]
plus a weekly Delta maintenance DAG (OPTIMIZE / Z-ORDER / VACUUM).

Each task calls the `lakesense` CLI, so the same code runs locally, in CI and in
Airflow (or MWAA). On AWS, swap the BashOperators for EmrServerless / Glue operators.
Bronze only reads files it has not seen before, so every run - and every retry - is
idempotent. Run `lakesense all` once to create the initial full load.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="lakesense_ai_daily",
    description="Medallion pipeline with AI data-quality triage and anomaly detection",
    start_date=datetime(2026, 6, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "pyspark", "ai"],
) as dag:
    # In production the source systems land files themselves; here the generator
    # stands in by appending one new day per run.
    land = BashOperator(task_id="land_new_files", bash_command="lakesense generate --append-days 1")
    bronze = BashOperator(task_id="bronze_incremental", bash_command="lakesense run --layer bronze")
    silver = BashOperator(task_id="silver_merge_scd2", bash_command="lakesense run --layer silver")
    gold = BashOperator(task_id="gold_marts", bash_command="lakesense run --layer gold")
    publish = BashOperator(task_id="publish_warehouse", bash_command="lakesense run --layer publish")
    dq = BashOperator(task_id="ai_dq_report", bash_command="lakesense dq-report")
    anomalies = BashOperator(task_id="ai_anomaly_report", bash_command="lakesense anomalies")
    drift = BashOperator(task_id="ai_schema_drift_report", bash_command="lakesense drift-report")

    land >> bronze >> silver >> gold >> publish >> [dq, anomalies, drift]

with DAG(
    dag_id="lakesense_weekly_maintenance",
    description="Delta OPTIMIZE / Z-ORDER and VACUUM",
    start_date=datetime(2026, 6, 1),
    schedule="0 4 * * 0",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "delta", "maintenance"],
) as maintenance_dag:
    BashOperator(task_id="optimize_and_vacuum", bash_command="lakesense maintain --vacuum")
