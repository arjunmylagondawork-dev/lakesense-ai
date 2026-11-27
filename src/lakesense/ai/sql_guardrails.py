"""Guardrails for LLM-generated SQL.

LLM output is untrusted input. Before anything runs we:
  1. parse it with sqlglot (must be valid DuckDB SQL),
  2. require exactly ONE statement that is a read-only SELECT / WITH / UNION,
  3. reject any DDL/DML/admin node anywhere in the tree,
  4. allow only whitelisted tables (CTE names are fine),
  5. enforce a row LIMIT.
Execution additionally uses a read-only DuckDB connection with external (file/network)
access disabled - defence in depth.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp


class GuardrailError(ValueError):
    """Base class: the SQL was rejected and must not run."""


class InvalidSQLError(GuardrailError):
    """SQL is malformed. Safe to send the message back to the LLM and retry."""


class UnsafeSQLError(GuardrailError):
    """SQL tries something forbidden. Fail closed: never retried, never executed."""


ANSI = re.compile(r"\x1b\[[0-9;]*m")


FORBIDDEN = tuple(
    getattr(exp, name) for name in (
        "Insert", "Update", "Delete", "Drop", "Create", "Alter", "Merge", "Command",
        "Copy", "Attach", "Detach", "Pragma", "Set", "Use", "TruncateTable", "Grant",
    ) if hasattr(exp, name)
)
FORBIDDEN_FUNCS = {"read_csv", "read_csv_auto", "read_parquet", "read_json", "read_json_auto",
                   "read_text", "read_blob", "glob", "httpfs", "getenv"}


def strip_fences(sql: str) -> str:
    """Models sometimes wrap SQL in ```sql fences or add a trailing semicolon."""
    sql = re.sub(r"^```(?:sql)?\s*|\s*```$", "", sql.strip(), flags=re.IGNORECASE)
    return sql.strip().rstrip(";").strip()


def _safe_limit(limit: exp.Expression | None) -> int | None:
    """Return the LIMIT value only if it is a plain integer literal (no expressions, no PERCENT)."""
    if limit is None or any(v for k, v in limit.args.items() if k != "expression"):
        return None
    value = limit.expression
    if isinstance(value, exp.Literal) and not value.is_string and value.name.isdigit():
        return int(value.name)
    return None


def validate_sql(sql: str, allowed_tables: list[str], max_rows: int = 200) -> str:
    sql = strip_fences(sql)
    try:
        statements = [s for s in sqlglot.parse(sql, read="duckdb") if s is not None]
    except sqlglot.errors.SqlglotError as e:          # ParseError, TokenError, ...
        raise InvalidSQLError(f"SQL does not parse: {ANSI.sub('', str(e)).splitlines()[0]}") from e
    if len(statements) != 1:
        raise UnsafeSQLError(f"Expected exactly one statement, got {len(statements)}")
    tree = statements[0]

    if not isinstance(tree, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise UnsafeSQLError(f"Only SELECT queries are allowed (got {type(tree).__name__})")
    for node in tree.walk():
        if isinstance(node, FORBIDDEN):
            raise UnsafeSQLError(f"Forbidden operation: {type(node).__name__}")
        if isinstance(node, exp.Func):
            fname = node.name.lower() if isinstance(node, exp.Anonymous) else (node.sql_name() or "").lower()
            if fname in FORBIDDEN_FUNCS:
                raise UnsafeSQLError(f"Forbidden function: {fname}")

    cte_names = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    allowed = {t.lower() for t in allowed_tables} | cte_names
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        if not name:
            raise UnsafeSQLError("Table functions / file reads are not allowed")
        if table.catalog or table.db.lower() not in ("", "main"):
            raise UnsafeSQLError(f"Qualified table names are not allowed: {table.sql(dialect='duckdb')}")
        if name not in allowed:
            raise UnsafeSQLError(f"Table '{table.name}' is not in the allowed list")

    if isinstance(tree, exp.Select):
        current = _safe_limit(tree.args.get("limit"))
        if current is None or current > max_rows:
            tree = tree.limit(max_rows)              # replaces any unsafe/oversized LIMIT
    else:
        tree = exp.select("*").from_(tree.subquery("q")).limit(max_rows)
    return tree.sql(dialect="duckdb")
