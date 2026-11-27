"""AI schema-drift report: what changed in the source files, and what to do about it."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from lakesense.ai import prompts
from lakesense.ai.llm import LLMClient, get_llm
from lakesense.config import Settings
from lakesense.logging_utils import get_logger

log = get_logger(__name__)


def summarise(drift: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse per-file drift records into one entry per (source, change)."""
    groups: dict[tuple, dict[str, Any]] = {}
    files: dict[tuple, list[str]] = defaultdict(list)
    for d in drift:
        key = (d["source"], tuple(d["new_columns"]), tuple(d["missing_required"]), tuple(d["missing_optional"]))
        groups[key] = {"source": d["source"], "new_columns": d["new_columns"],
                       "missing_required": d["missing_required"], "missing_optional": d["missing_optional"],
                       "accepted": d["accepted"]}
        files[key].append(d["file"])
    return [{**g, "files": sorted(files[k]), "file_count": len(files[k])} for k, g in groups.items()]


def explain(settings: Settings, llm: LLMClient | None = None) -> Path:
    llm = llm or get_llm()
    reports = settings.paths.reports
    path = reports / "schema_drift.json"
    drift = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    changes = summarise(drift)
    contracts = settings.section("contracts")
    body = llm.complete(
        prompts.DRIFT_SYSTEM,
        f"Contracts:\n{json.dumps(contracts, indent=2)}\nObserved changes:\n{json.dumps(changes, indent=2)}",
        task="schema_drift", context={"changes": changes, "contracts": contracts},
    )
    out = reports / "schema_drift_report.md"
    out.write_text(f"# Schema Drift Report\n\n_LLM provider: `{llm.name}`_\n\n{body}\n", encoding="utf-8")
    log.info("Schema drift: %d distinct change(s) -> %s", len(changes), out)
    return out
