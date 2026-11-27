# Roadmap

Each phase is a self-contained upgrade that can be shipped as its own pull request.

## ✅ Phase 1: Foundation (done)
- Medallion pipeline in PySpark with quarantine and lineage
- Config-driven DQ engine, AI incident report
- Weekday-adjusted anomaly detection + AI narrative
- Guarded text-to-SQL with self-correction and audit log
- Streamlit app, Airflow DAG, Docker, CI, 40+ tests

## Phase 2: Production table formats & incremental loads
- [ ] Replace Parquet with **Delta Lake or Apache Iceberg** (ACID, time travel, schema evolution)
- [ ] **Incremental processing**: only new landing files (watermark / file manifest), `MERGE INTO` upserts
- [ ] **SCD Type 2** for `customers` (track segment changes over time)
- [ ] Schema-drift detection in bronze, with an LLM-suggested mapping for new columns

## Phase 3: Modern data stack
- [ ] Rebuild gold marts as **dbt** models (`dbt-duckdb` locally, `dbt-snowflake` in cloud) with dbt tests and docs
- [ ] **Snowflake** target: load silver via `COPY INTO`, compare with DuckDB results
- [ ] Data contracts (YAML) checked in CI

## Phase 4: Cloud deployment (AWS)
- [ ] **Terraform**: S3 buckets, Glue jobs, Glue Catalog, Athena workgroup, IAM least-privilege roles, KMS
- [ ] Run silver/gold on **EMR Serverless** or **Glue**, orchestrate with **MWAA** or Step Functions
- [ ] Switch AI provider to **Bedrock**; send DQ reports to Slack/SNS

## Phase 5: Streaming
- [ ] Kafka (or Redpanda) producer for orders; **Spark Structured Streaming** into bronze
- [ ] Real-time anomaly alerts on a 5-minute revenue window

## Phase 6: Stronger AI
- [ ] **Text-to-SQL evaluation set** (30+ questions with gold SQL), execution accuracy tracked in CI
- [ ] **RAG over the data catalog**: embed table/column descriptions and dbt docs so the agent picks the right tables
- [ ] LLM-generated DQ rule *suggestions* from data profiling (human-approved before going into YAML)
- [ ] Cost and latency tracking per LLM call; caching of repeated questions
