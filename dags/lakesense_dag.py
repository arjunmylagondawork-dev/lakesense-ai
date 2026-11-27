"""Airflow DAG for LakeSense AI.

generate -> bronze -> silver -> gold -> publish -> [dq_report, anomalies]

Each task calls the `lakesense` CLI, so the same code runs locally, in CI and in
Airflow (or MWAA). On AWS, swap the BashOperators for EmrServerless / Glue operators.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
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
    generate = BashOperator(task_id="generate_raw", bash_command="lakesense generate")
    bronze = BashOperator(task_id="bronze", bash_command="lakesense run --layer bronze")
    silver = BashOperator(task_id="silver_with_dq", bash_command="lakesense run --layer silver")
    gold = BashOperator(task_id="gold_marts", bash_command="lakesense run --layer gold")
    publish = BashOperator(task_id="publish_warehouse", bash_command="lakesense run --layer publish")
    dq = BashOperator(task_id="ai_dq_report", bash_command="lakesense dq-report")
    anomalies = BashOperator(task_id="ai_anomaly_report", bash_command="lakesense anomalies")

    generate >> bronze >> silver >> gold >> publish >> [dq, anomalies]
