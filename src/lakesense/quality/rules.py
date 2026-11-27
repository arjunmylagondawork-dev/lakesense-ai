"""Config-driven data-quality engine for Spark DataFrames.

Every rule compiles to a boolean Spark Column that is TRUE when a row VIOLATES
the rule. All rules for a table are evaluated in a single pass and the names of
the violated rules are collected into an array column `_dq_failed` - so
quarantined rows always say *why* they were rejected.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class Rule:
    name: str
    type: str
    column: str
    severity: str = "error"
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Rule:
        d = dict(d)
        core = {k: d.pop(k) for k in ("name", "type", "column") if k in d}
        severity = d.pop("severity", "error")
        return cls(**core, severity=severity, params=d)


@dataclass
class RuleResult:
    table: str
    rule: str
    type: str
    column: str
    severity: str
    total_rows: int
    failed_rows: int
    sample_values: list[str]

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total_rows == 0 else 1 - self.failed_rows / self.total_rows

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "pass_rate": round(self.pass_rate, 5)}


def violation_expr(rule: Rule) -> Column:
    c = F.col(rule.column)
    p = rule.params
    if rule.type == "not_null":
        return c.isNull() | (F.trim(c.cast("string")) == "")
    if rule.type == "range":
        cond = F.lit(False)
        if "min" in p:
            cond = cond | (c < F.lit(p["min"]))
        if "max" in p:
            cond = cond | (c > F.lit(p["max"]))
        return c.isNull() | cond
    if rule.type == "accepted_values":
        return c.isNull() | ~c.isin(p["values"])
    if rule.type == "regex":
        return c.isNotNull() & ~c.rlike(p["pattern"])
    if rule.type == "not_future":
        return c.isNull() | (c > F.current_timestamp())
    if rule.type == "referential":
        # resolved via a left join in apply_rules; column added there
        return F.col(f"_ref_missing_{rule.name}")
    raise ValueError(f"Unknown rule type: {rule.type}")


def apply_rules(
    df: DataFrame,
    rules: list[Rule],
    ref_dfs: dict[str, DataFrame] | None = None,
) -> DataFrame:
    """Return df with `_dq_failed` (all violations) and `_dq_errors` (error-severity only)."""
    ref_dfs = ref_dfs or {}
    helper_cols = []
    for rule in rules:
        if rule.type != "referential":
            continue
        ref = ref_dfs[rule.params["ref_table"]]
        flag = f"_ref_missing_{rule.name}"
        keys = ref.select(F.col(rule.params["ref_column"]).alias("_ref_key")).distinct()
        keys = keys.withColumn(flag, F.lit(False))
        df = (
            df.join(F.broadcast(keys), df[rule.column] == keys["_ref_key"], "left")
            .drop("_ref_key")
            .withColumn(flag, F.coalesce(F.col(flag), F.col(rule.column).isNotNull()))
        )
        helper_cols.append(flag)

    def names(selected: list[Rule]) -> Column:
        arr = F.array(*[F.when(violation_expr(r), F.lit(r.name)) for r in selected]) if selected \
            else F.array().cast("array<string>")
        return F.filter(arr, lambda x: x.isNotNull())

    df = df.withColumn("_dq_failed", names(rules))
    df = df.withColumn("_dq_errors", names([r for r in rules if r.severity == "error"]))
    return df.drop(*helper_cols)


def summarize(table: str, checked: DataFrame, rules: list[Rule], sample_size: int = 5) -> list[RuleResult]:
    """Aggregate violation counts per rule (one Spark job) + a few sample bad values.

    Caching is the caller's responsibility (silver caches `checked` once and reuses it).
    """
    total = checked.count()
    aggs = [F.sum(F.array_contains("_dq_failed", r.name).cast("int")).alias(r.name) for r in rules]
    counts = checked.agg(*aggs).first().asDict() if rules else {}
    results = []
    for r in rules:
        failed = int(counts.get(r.name) or 0)
        samples: list[str] = []
        if failed:
            samples = [
                str(row[0]) for row in
                checked.filter(F.array_contains("_dq_failed", r.name))
                .select(F.col(r.column).cast("string")).distinct().limit(sample_size).collect()
            ]
        results.append(RuleResult(table, r.name, r.type, r.column, r.severity, total, failed, samples))
    return results
