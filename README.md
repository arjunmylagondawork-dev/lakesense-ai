# 🌊 LakeSense AI

**An AI-augmented PySpark + Delta Lake lakehouse.** An incremental medallion pipeline
(bronze → silver → gold) that enforces source schema contracts, validates its own data,
quarantines bad records with reasons, upserts late corrections with `MERGE`, keeps customer
history as SCD Type 2, detects business anomalies, and uses an LLM to **explain data-quality
incidents and schema drift**, **narrate anomalies** and answer **plain-English questions with
guarded text-to-SQL**.

[![CI](https://github.com/arjunmylagondawork-dev/lakesense-ai/actions/workflows/ci.yml/badge.svg)](https://github.com/arjunmylagondawork-dev/lakesense-ai/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.x-orange)
![Delta Lake](https://img.shields.io/badge/tables-Delta%20Lake-00ADD4)
![DuckDB](https://img.shields.io/badge/serving-DuckDB-yellow)
![LLM](https://img.shields.io/badge/LLM-OpenAI%20%7C%20Anthropic%20%7C%20Bedrock%20%7C%20mock-purple)

> 📖 **Visual walkthrough:** [docs/how-it-works.html](docs/how-it-works.html) is an interactive page covering the
> pipeline stages, the journey of real bad records, anomaly results and a SQL guardrail playground.
> Live: **https://arjunmylagondawork-dev.github.io/lakesense-ai/**

> Runs fully offline in about 2 minutes: no cloud account and no API key needed (a deterministic
> mock LLM is built in). Plug in OpenAI, Anthropic or Amazon Bedrock with one environment variable.

---

## Why this project

Most "AI + data" demos skip the hard part: **the data is messy**. LakeSense starts from
deliberately dirty source files (nulls, negative quantities, broken timestamps, orphan keys,
duplicates, text in numeric columns) and shows how a production-style pipeline handles them.
Then it applies AI where it actually helps a data team:

| Problem a data team has | What LakeSense does |
|---|---|
| "Why did 1,800 rows get rejected last night?" | Config-driven DQ engine + **LLM incident report** with root cause, impact and fix snippets |
| "Revenue looks weird on Tuesday" | **Robust statistical anomaly detection** (weekday-adjusted median/MAD z-score) + LLM narrative |
| "Can you pull top customers by country for me?" | **Text-to-SQL agent** with SQL guardrails, read-only execution, self-correction and audit log |
| "The partner feed changed its columns again" | **Schema contracts** reject breaking files, keep new columns, and an **LLM drift report** says what to do |
| "Yesterday's orders got corrected today" | **Incremental loads**: only new files are read, corrections are **MERGE**d into silver |

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        A1[customers.jsonl<br/>CDC updates]
        A2[products.csv]
        A3[orders_YYYY-MM-DD.csv<br/>daily landing]
    end

    subgraph Lakehouse["PySpark + Delta Lake"]
        C{{Schema contract<br/>check per file}}
        B[Bronze<br/>append-only, raw strings<br/>+ lineage, new files only]
        S[Silver<br/>typed, validated<br/>MERGE upserts]
        H[customer_history<br/>SCD Type 2]
        Q[(Quarantine<br/>rows + failed rules)]
        G[Gold marts<br/>daily_revenue, customer_rfm,<br/>product_performance]
    end

    subgraph Serving
        W[(DuckDB warehouse<br/>read-only)]
    end

    subgraph AI["AI layer (pluggable LLM)"]
        D[DQ explainer]
        N[Anomaly detector<br/>+ narrator]
        T[Text-to-SQL agent<br/>guardrails, retries]
    end

    A1 & A2 & A3 --> C --> B --> S --> G --> W
    C -- breaking change --> R[(Rejected file<br/>+ drift report)]
    S --> H --> W
    S -- error rules --> Q
    S -- dq_results.json --> D
    W --> N
    W <--> T
    D & N & T --> UI[Streamlit app / CLI / reports]
    AF[Airflow DAG] -. orchestrates .-> B & S & G & W & D & N
```

## Data-engineering highlights

- **Medallion architecture on Delta Lake**: bronze is an append-only, replayable copy with lineage
  columns (`_source_file`, `_ingest_ts`, `_batch_id`); silver is typed and clean; gold is business-ready.
  A storage layer (`storage.py`) runs the same code on **Delta Lake** (default) or plain Parquet, and CI tests both.
- **Incremental and idempotent**: a file manifest tracks what was ingested (name, size, mtime), so a
  re-run or a retry is a no-op and new daily files are picked up automatically. Silver processes only
  pending batches and **upserts with `MERGE`**: late-arriving status corrections update existing rows,
  and a newer row always wins.
- **SCD Type 2** `customer_history`: a new version only when a tracked attribute (segment, country,
  email) actually changes, with chained `valid_from`/`valid_to`, one `is_current` row per customer,
  and out-of-order updates ignored.
- **Schema contracts and drift**: every new file is checked before ingestion. A missing required column
  rejects that file only; a new column is kept in bronze (schema evolution) and reported.
- **Delta features in use**: `MERGE INTO`, schema evolution, time travel (`lakesense history silver.orders
  --version 0`), `OPTIMIZE ... ZORDER BY`, `VACUUM`. The serving layer reads the **Delta log** to load
  only live files, never ones that a later MERGE removed.
- **ANSI-safe parsing**: `try_cast` / `try_to_timestamp` turn bad values into NULL instead of
  crashing the job. Two timestamp formats (ISO and a legacy `dd/MM/yyyy HH:mm`) are supported.
- **Config-driven data quality** (`config/pipeline.yaml`): `not_null`, `range`, `accepted_values`,
  `regex`, `not_future`, `referential`. All rules are evaluated in **one pass** into an array of
  failed rule names, so quarantined rows always say *why* they were rejected. `error` rules block a row; `warn` rules only report it.
- **Referential integrity with broadcast joins**, dimensions loaded before facts, and visible
  cascading failures (a quarantined product orphans its order lines).
- **Window functions everywhere**: `row_number` dedup (latest record wins), 7-day moving average,
  `ntile` RFM scoring, `dense_rank` in-category ranking, revenue share by partition.
- **Partitioned tables** (`order_date`), AQE enabled.
- **Atomic warehouse publish**: built to a temp file and swapped in, so readers never see half a warehouse.
- **Orchestration**: a daily incremental Airflow DAG and a weekly Delta maintenance DAG, with retries
  and exponential backoff; every task calls the same CLI.
- **Tested**: 50+ pytest tests (every cleaning function, gold mart and rule type, MERGE and SCD2 semantics,
  an incremental run with drift, Delta time travel, a guardrail security suite, the AI retry paths, and
  end-to-end runs), plus ruff lint. GitHub Actions runs it all on Parquet and on Delta.

## AI highlights

- **Provider-agnostic LLM client**: `mock | openai | anthropic | bedrock` (Bedrock uses the Converse API).
  SDKs are optional, lazily imported dependencies.
- **LLM output is treated as untrusted input.** Generated SQL is parsed with `sqlglot` and must be a
  single `SELECT`; DDL/DML, `ATTACH`, `COPY`, `PRAGMA`, file-reading functions and non-whitelisted
  tables and schema-qualified names (`other_db.main.x`) are rejected; any `LIMIT` that is not a plain
  integer (`LIMIT 5+1000000`, `LIMIT 100 PERCENT`) is replaced. Execution uses a **read-only** DuckDB
  connection with **file and network access disabled**, so even SQL that slipped past the parser can't read files.
- **Self-correction loop**: malformed SQL and database errors are fed back to the model (up to N retries).
  Unsafe SQL is *never* retried; it fails closed. Questions the tables can't answer get a clear
  "can't answer" instead of a made-up query.
- **Statistics detect, the LLM explains**: anomalies are found with a weekday-adjusted robust z-score
  (no hallucinated alerts); the LLM only writes the explanation, using a per-category breakdown as grounding.
- **Auditability**: every question, generated SQL, outcome, attempt count and latency is logged to
  `reports/query_log.jsonl`.

## Quick start

Requirements: Python 3.10+, Java 17+ (for Spark) and internet access on the first run (Spark downloads the
Delta Lake jars once). Offline? Set `LAKESENSE_TABLE_FORMAT=parquet`.

```bash
git clone https://github.com/arjunmylagondawork-dev/lakesense-ai.git
cd lakesense-ai
python -m venv .venv && source .venv/bin/activate
pip install -e ".[app,dev]"

lakesense all                             # generate data, run every layer, write AI reports
                                          # (run from the project folder, or set LAKESENSE_HOME)
lakesense ask "top 5 customers by spend"  # natural language -> SQL -> answer

# incremental: land 7 more days (with late corrections and a new source column), then load only those
lakesense generate --append-days 7 --drift new-column
lakesense run                             # bronze picks up new files, silver MERGEs, SCD2 updates
lakesense drift-report                    # AI explanation of the schema change
lakesense history silver.orders           # Delta table versions (time travel)
streamlit run app/streamlit_app.py        # dashboard at http://localhost:8501
pytest                                    # run the test suite
```

Or with Docker: `docker compose up --build`, then open http://localhost:8501.

### Use a real LLM

```bash
pip install -e ".[openai]"      # or .[anthropic] / .[bedrock]
cp .env.example .env            # set LAKESENSE_LLM_PROVIDER=openai and OPENAI_API_KEY=...
lakesense dq-report             # .env is loaded automatically (real env vars take priority)
```

## Example output

```text
$ lakesense ask "top 5 customers by spend"

SQL (1 attempt(s), 83 ms):
SELECT customer_id, name, country, frequency, monetary, rfm_segment
FROM customer_rfm ORDER BY monetary DESC LIMIT 5

customer_id            name country  frequency  monetary rfm_segment
     C01305       Kabir Rao      SG         15  23518.79    champion
     C01244     Rohan Patel      IN         22  21792.88    champion
     ...
```

Sample reports from a real run are in [`reports/sample/`](reports/sample/):
[DQ incident report](reports/sample/dq_report.md), [anomaly report](reports/sample/anomaly_report.md)
and [schema drift report](reports/sample/schema_drift_report.md).
The generator injects a revenue spike on 2026-07-16 and an outage-style drop on 2026-08-28; both are detected.

## Project structure

```
lakesense-ai/
├── config/pipeline.yaml          # storage format, contracts, DQ rules, anomaly + text-to-SQL settings
├── src/lakesense/
│   ├── generator/                # seeded synthetic dirty data (+ incremental append mode)
│   ├── pipeline/                 # bronze.py, silver.py, scd2.py, gold.py
│   ├── quality/                  # rules.py (DQ engine), contracts.py (schema drift)
│   ├── storage.py                # Delta Lake / Parquet tables: read, write, MERGE, history, OPTIMIZE
│   ├── state.py                  # incremental state: ingested files, processed batches
│   ├── serving/warehouse.py      # DuckDB publish + schema introspection
│   ├── ai/                       # llm.py, prompts.py, dq_explainer.py, anomaly.py, schema_drift.py,
│   │                             # text_to_sql.py, sql_guardrails.py, mock_responses.py
│   └── cli.py                    # `lakesense` command
├── app/streamlit_app.py          # dashboard + "Ask your data"
├── dags/lakesense_dag.py         # Airflow orchestration
├── tests/                        # unit, security and end-to-end tests
├── docs/                         # architecture decisions and roadmap
├── Dockerfile, docker-compose.yml, Makefile
└── .github/workflows/ci.yml
```

## CLI

| Command | What it does |
|---|---|
| `lakesense generate [--append-days N] [--drift new-column\|missing-column]` | Write synthetic raw files, or append new days |
| `lakesense run [--layer ...] [--mode incremental\|full]` | Run pipeline layers (incremental by default) |
| `lakesense history LAYER.TABLE [--version N]` | Delta table history / time travel |
| `lakesense maintain [--vacuum]` | Delta `OPTIMIZE` + `ZORDER BY`, optional `VACUUM` |
| `lakesense drift-report` | AI schema-drift report → `reports/schema_drift_report.md` |
| `lakesense dq-report` | AI data-quality incident report → `reports/dq_report.md` |
| `lakesense anomalies` | Anomaly detection + narrative → `reports/anomaly_report.md` |
| `lakesense ask "<question>"` | Guarded text-to-SQL |
| `lakesense all` | Everything, end to end |

## Design decisions

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). In short: Delta Lake + DuckDB keep the project free and
cloneable; each piece maps one-to-one to AWS (S3 + Glue/EMR + Athena/Redshift + MWAA + Bedrock) or to Databricks.

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md). Done: Delta Lake, incremental MERGE loads, SCD2, schema contracts.
Next: dbt models, Snowflake target, Terraform for AWS, Kafka streaming, a text-to-SQL evaluation set and RAG
over a data catalog.

## Notes

The dataset is synthetic and generated locally, so no real customer data is used. The default `mock` LLM
returns deterministic, template-based answers so that the full AI flow (prompting, guardrails, retries,
reporting) runs and is tested without an API key. Switch providers for genuine model output.

## License

MIT © Arjun Mylagonda
