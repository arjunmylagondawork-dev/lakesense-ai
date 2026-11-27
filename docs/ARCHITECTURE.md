# Architecture & design decisions

## Layers

| Layer | Storage | Contract |
|---|---|---|
| Raw | `data/raw/` CSV + JSON lines | Whatever the source sends |
| Bronze | Parquet, all `string` | Faithful, replayable copy + `_source_file`, `_ingest_ts`, `_batch_id` |
| Silver | Parquet, typed, `orders` partitioned by `order_date` | Clean, de-duplicated, passes all `error` rules |
| Quarantine | Parquet | Rejected rows + `_dq_failed`, `_dq_errors`, `_quarantined_at` |
| Gold | Parquet | Business marts: `daily_revenue`, `customer_rfm`, `product_performance` |
| Serving | DuckDB file | Gold + silver tables for SQL / BI / AI, opened read-only by consumers |

## Key decisions

**1. Bronze stores strings only.** Casting at ingest loses information when a value is bad
(`"two"` silently becomes NULL and you can no longer tell what arrived). Keeping strings means
silver logic can be fixed and replayed from bronze without going back to the source.

**2. ANSI-safe casting.** Spark 4 enables ANSI mode by default, so a plain `cast` of `"two"` to
`INT` fails the whole job. `try_cast` / `try_to_timestamp` give NULLs that the DQ rules then catch
and route to quarantine. One bad row should not stop the pipeline.

**3. One-pass, array-based DQ.** Each rule compiles to a boolean column; violated rule names are
collected into an array with `F.filter(F.array(...))`. That is one scan per table regardless of rule
count, and every rejected row carries its reasons. Rules live in YAML, so adding a check needs no code.

**4. Error vs. warn severity.** Blocking every imperfection would throw away revenue (an order
whose customer record is late is still a real order). `warn` rules are reported and scored, but the row flows on.

**5. Dimensions before facts.** Referential checks run against the *silver* dimension, so a
dimension row that was itself quarantined shows up as orphaned facts. The DQ report calls out
this cascade explicitly.

**6. Statistics detect, LLM explains.** LLMs are poor at arithmetic and can invent alerts.
Detection is a modified z-score (`0.6745 * (x - median) / MAD`) on a trailing window, after dividing by a
day-of-week index so busy weekends are not flagged. The LLM receives the numbers plus a per-category
breakdown and only writes the explanation.

**7. LLM output is untrusted.** Text-to-SQL output goes through `sqlglot` parsing, a single-statement
rule, an AST walk that rejects DDL/DML/admin nodes and file-reading functions, a table whitelist (CTEs
allowed), and a forced `LIMIT`, and it runs on a read-only connection. Unsafe SQL is never retried.

**8. Mock provider by default.** A reviewer can clone and run everything in minutes, and CI tests the
whole AI flow deterministically. Real providers sit behind the same `LLMClient.complete()` interface.

## Mapping to AWS

| Local | AWS equivalent |
|---|---|
| `data/` Parquet | S3 (raw / bronze / silver / gold prefixes) |
| PySpark local | AWS Glue jobs or EMR Serverless |
| DuckDB | Athena (Glue Data Catalog) or Redshift Serverless |
| Airflow DAG | Amazon MWAA |
| `bedrock` provider | Amazon Bedrock (Converse API) |
| `reports/` | S3 + SNS/Slack notification |
| Quarantine | S3 prefix + Athena table for triage |
