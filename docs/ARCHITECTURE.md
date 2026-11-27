# Architecture & design decisions

## Layers

| Layer | Storage | Contract |
|---|---|---|
| Raw | `data/raw/` CSV + JSON lines | Whatever the source sends; each file checked against its contract |
| Bronze | Delta (or Parquet), all `string`, append-only | Faithful, replayable copy + `_source_file`, `_ingest_ts`, `_batch_id` |
| Silver | Delta, typed, `orders` partitioned by `order_date` | Clean, de-duplicated, passes all `error` rules; upserted with MERGE |
| customer_history | Delta | SCD Type 2 versions of customer segment / country / email |
| Quarantine | Delta, append-only | Rejected rows + `_dq_failed`, `_dq_errors`, `_quarantined_at` |
| Gold | Delta | Business marts: `daily_revenue`, `customer_rfm`, `product_performance` |
| State | `lake/_state/state.json` | Ingested files (name, size, mtime) and processed batches |
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

**4b. NULL keys are never de-duplicated away.** Deduplication only applies to rows that have a key;
rows with a NULL key pass through so the `not_null` rule quarantines each one with a reason.

**5. Dimensions before facts.** Referential checks run against the *silver* dimension, so a
dimension row that was itself quarantined shows up as orphaned facts. The DQ report calls out
this cascade explicitly.

**6. Statistics detect, LLM explains.** LLMs are poor at arithmetic and can invent alerts.
Detection is a modified z-score (`0.6745 * (x - median) / MAD`) on a trailing window, after dividing by a
day-of-week index so busy weekends are not flagged. The LLM receives the numbers plus a per-category
breakdown and only writes the explanation.

**7. LLM output is untrusted.** Text-to-SQL output goes through `sqlglot` parsing, a single-statement
rule, an AST walk that rejects DDL/DML/admin nodes and file-reading functions, a table whitelist (CTEs
allowed, no other schemas or catalogs), and a forced plain-integer `LIMIT`. It runs on a read-only
connection with external access disabled. Malformed SQL is retried with the parser error; unsafe SQL never is.

**8. Mock provider by default.** A reviewer can clone and run everything in minutes, and CI tests the
whole AI flow deterministically. Real providers sit behind the same `LLMClient.complete()` interface.

**9. Incremental by default, idempotent always.** Bronze ingests only files whose name, size and mtime
it has not seen; silver processes only bronze batches it has not processed. Re-running any step, or
retrying it in Airflow, never duplicates data. `--mode full` rebuilds everything from raw files.

**10. MERGE with "newer wins".** Silver upserts on the business key and updates a row only when the
incoming record is at least as new (`_ingest_ts`, or `updated_at` for customers). A late status
correction updates the original order line; an old replay can never overwrite newer data.

**11. SCD2 only on real change.** A hash of the tracked attributes is compared with the previous
version, so re-sending identical data creates no new version. Several changes in one batch are all
kept in order; updates older than the current version are ignored.

**12. Contracts before ingestion.** Checking headers before Spark reads a file means a broken file
is rejected on its own, without failing the batch. Files are read in groups of identical header and
combined by column name, so an extra column can never shift values into the wrong column.

**13. Gold is rebuilt, silver is merged.** Gold marts are small aggregates with global windows (RFM
quintiles, rankings), so they are recomputed from silver on every run. That is cheaper and simpler than
incrementally maintaining them, and they are always consistent with silver.

**14. Serving reads the Delta log.** A Delta table directory still contains files that later MERGEs
logically removed. The publish step asks the Delta log (via `deltalake`) for the live file list, so
DuckDB never double-counts.

## Mapping to AWS

| Local | AWS equivalent |
|---|---|
| `data/` Delta tables | S3 (raw / bronze / silver / gold prefixes), or Databricks Unity Catalog tables |
| PySpark local | AWS Glue jobs or EMR Serverless |
| DuckDB | Athena (Glue Data Catalog) or Redshift Serverless |
| Airflow DAG | Amazon MWAA |
| `bedrock` provider | Amazon Bedrock (Converse API) |
| `reports/` | S3 + SNS/Slack notification |
| Quarantine | S3 prefix + Athena table for triage |
