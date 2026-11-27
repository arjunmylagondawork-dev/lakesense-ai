"""'Ask your data': natural-language questions -> safe SQL -> answer.

Flow:  question -> LLM (schema in prompt) -> guardrails -> read-only DuckDB
       -> on error, feed the error back to the LLM and retry (self-correction)
       -> LLM summary of the result rows.
Every attempt is logged to reports/query_log.jsonl for auditability.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd

from lakesense.ai import prompts
from lakesense.ai.llm import LLMClient, get_llm
from lakesense.ai.sql_guardrails import UnsafeSQLError, validate_sql
from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.serving.warehouse import connect_readonly, schema_description

log = get_logger(__name__)


@dataclass
class Answer:
    question: str
    sql: str | None
    data: pd.DataFrame | None
    summary: str
    attempts: int
    error: str | None = None
    latency_ms: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None


class TextToSQLAgent:
    def __init__(self, settings: Settings, llm: LLMClient | None = None,
                 con: duckdb.DuckDBPyConnection | None = None):
        cfg = settings.section("text_to_sql")
        self.settings = settings
        self.allowed = cfg.get("allowed_tables", [])
        self.max_rows = cfg.get("max_rows", 200)
        self.max_retries = cfg.get("max_retries", 2)
        self.llm = llm or get_llm()
        self.con = con or connect_readonly(settings)
        self.schema = schema_description(self.con, self.allowed)

    def _generate(self, question: str, prev_sql: str | None = None, error: str | None = None) -> str:
        system = prompts.SQL_SYSTEM.format(schema=self.schema)
        if prev_sql is None:
            prompt = f"Question: {question}"
        else:
            prompt = prompts.SQL_FIX.format(question=question, sql=prev_sql, error=error)
        return self.llm.complete(system, prompt, task="text_to_sql",
                                 context={"question": question, "previous_sql": prev_sql, "error": error})

    def ask(self, question: str) -> Answer:
        start = time.perf_counter()
        history: list[dict[str, Any]] = []
        sql, error = None, None
        for attempt in range(1, self.max_retries + 2):
            raw = self._generate(question, sql, error) if attempt > 1 else self._generate(question)
            try:
                sql = validate_sql(raw, self.allowed, self.max_rows)
                df = self.con.execute(sql).fetchdf()
                summary = self.llm.complete(
                    prompts.SUMMARY_SYSTEM,
                    f"Question: {question}\nColumns: {list(df.columns)}\nRows:\n{df.head(20).to_string()}",
                    task="summarize_result",
                    context={"columns": list(df.columns), "rows": df.head(20).values.tolist()},
                )
                history.append({"attempt": attempt, "sql": sql, "status": "ok"})
                ans = Answer(question, sql, df, summary, attempt, history=history,
                             latency_ms=int((time.perf_counter() - start) * 1000))
                self._audit(ans)
                return ans
            except UnsafeSQLError as e:
                # unsafe output is never retried into execution: fail closed
                history.append({"attempt": attempt, "sql": raw, "status": "blocked", "error": str(e)})
                ans = Answer(question, raw, None, "Query blocked by guardrails.", attempt,
                             error=f"Blocked: {e}", history=history,
                             latency_ms=int((time.perf_counter() - start) * 1000))
                self._audit(ans)
                return ans
            except duckdb.Error as e:
                error = str(e).splitlines()[0]
                sql = sql or raw
                history.append({"attempt": attempt, "sql": sql, "status": "error", "error": error})
                log.warning("Attempt %d failed: %s", attempt, error)
        ans = Answer(question, sql, None, "Could not answer the question.", len(history),
                     error=error, history=history, latency_ms=int((time.perf_counter() - start) * 1000))
        self._audit(ans)
        return ans

    def _audit(self, ans: Answer) -> None:
        path = self.settings.paths.reports / "query_log.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now(timezone.utc).isoformat(), "provider": self.llm.name,
            "question": ans.question, "sql": ans.sql, "ok": ans.ok, "error": ans.error,
            "attempts": ans.attempts, "rows": None if ans.data is None else len(ans.data),
            "latency_ms": ans.latency_ms,
        }
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
