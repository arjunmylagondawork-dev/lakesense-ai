# Roadmap

Each phase is a self-contained upgrade that can be shipped as its own pull request.

## ✅ Phase 1: Foundation (done)
- Medallion pipeline in PySpark with quarantine and lineage
- Config-driven DQ engine, AI incident report
- Weekday-adjusted anomaly detection + AI narrative
- Guarded text-to-SQL with self-correction and audit log
- Streamlit app, Airflow DAG, Docker, CI, 40+ tests

## ✅ Phase 2: Production table formats & incremental loads (done)
- [x] **Delta Lake** tables (ACID, time travel, schema evolution, OPTIMIZE / Z-ORDER / VACUUM), Parquet fallback
- [x] **Incremental processing**: file manifest, idempotent re-runs, `MERGE INTO` upserts of late corrections
- [x] **SCD Type 2** `customer_history` (segment / country / email changes over time)
- [x] Source **schema contracts** + drift detection in bronze, with an LLM drift report and rename hints
- [ ] Apache Iceberg as a third table format

## Phase 3: Modern data stack (in progress)
- [x] **dbt** analytics models (`dbt-duckdb`): staging, `fct_order_lines`, `dim_customer`, `mart_category_daily`
- [x] Enforced **model contracts**, 25+ data tests, Spark-vs-dbt revenue reconciliation, release gate on publish
- [x] Point-in-time join to SCD2 history in the fact table
- [ ] **Snowflake** target via `dbt-snowflake`: load silver with `COPY INTO`, run the same dbt project
- [ ] dbt docs site published with GitHub Pages

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
