"""Deterministic responses for the mock LLM provider.

They imitate what a real model returns for each task so that the full AI flow
(prompting, parsing, guardrails, retries, reporting) is exercised offline.
Swap LAKESENSE_LLM_PROVIDER to a real provider for genuine model output.
"""

from __future__ import annotations

import re
from typing import Any

ROOT_CAUSES = {
    "not_null": ("Source system sends blank/NULL keys - often a failed lookup or an optional "
                 "form field upstream.",
                 "Reject at source or enrich via a lookup; keep quarantining so facts never lose their key.",
                 "df.filter(F.col('{col}').isNull()).groupBy('_source_file').count()"),
    "range": ("Out-of-range numeric values - sign errors (refunds sent as negative quantities), "
              "zero prices from promo/test orders, or non-numeric text that failed to parse.",
              "Model refunds as a separate status instead of negative quantities; reject test orders at source.",
              "df.filter((F.col('{col}') <= 0) | F.col('{col}').isNull()).select('{col}', '_source_file')"),
    "accepted_values": ("Free-text or un-normalised codes (casing, typos, new enum values the "
                        "pipeline does not know yet).",
                        "Agree a code list with the source team; map known variants in silver and alert on new ones.",
                        "df.groupBy('{col}').count().orderBy(F.desc('count'))"),
    "regex": ("Malformed strings - e.g. emails without '@' typed into a CRM.",
              "Validate in the capture UI; mark contacts unreachable rather than dropping them.",
              "df.filter(~F.col('{col}').rlike(PATTERN)).select('{col}')"),
    "not_future": ("Timestamps in the future - placeholder dates like 2099-12-31 or clock/timezone bugs.",
                   "Treat sentinel dates as NULL at source; enforce UTC on the producer side.",
                   "df.filter(F.col('{col}') > F.current_timestamp()).select('{col}')"),
    "referential": ("Orphan foreign keys - the referenced dimension row is missing or was itself "
                    "quarantined (cascading failure), or the fact arrived before the dimension.",
                    "Load dimensions first, add late-arriving-dimension handling (placeholder member "
                    "+ reprocess), and fix the quarantined dimension rows.",
                    "facts.join(dim, '{col}', 'left_anti').select('{col}').distinct()"),
}


def dq_explanation(ctx: dict[str, Any]) -> str:
    results = ctx.get("results", [])
    failing = sorted([r for r in results if r["failed_rows"] > 0], key=lambda r: -r["failed_rows"])
    errors = [r for r in failing if r["severity"] == "error"]
    total_checks = len(results)
    health = ctx.get("health_score", 100.0)

    out = ["## Executive summary", ""]
    if not failing:
        out.append(f"All {total_checks} checks passed. Health score **{health:.1f}/100**.")
        return "\n".join(out)
    worst = failing[0]
    out.append(
        f"{len(failing)} of {total_checks} checks found issues ({len(errors)} blocking). "
        f"Overall health score is **{health:.1f}/100**. The largest problem is "
        f"`{worst['rule']}` on `{worst['table']}` with {worst['failed_rows']:,} rows "
        f"({(1 - worst['pass_rate']) * 100:.2f}%). Blocking rows were quarantined, so gold "
        f"metrics are not polluted, but revenue may be understated by the quarantined volume."
    )
    out += ["", "## Findings", "",
            "| Rule | Table | Failed rows | Severity | Likely root cause |",
            "|---|---|---:|---|---|"]
    for r in failing:
        cause = ROOT_CAUSES.get(r["type"], ("Unknown",))[0].split(" - ")[0]
        out.append(f"| `{r['rule']}` | {r['table']} | {r['failed_rows']:,} | {r['severity']} | {cause} |")

    out += ["", "## Details and fixes"]
    for r in failing:
        cause, action, snippet = ROOT_CAUSES.get(r["type"], ("Unknown cause.", "Investigate.", ""))
        samples = ", ".join(f"`{s}`" for s in r.get("sample_values", [])[:3]) or "n/a"
        out += [
            "", f"### `{r['rule']}` ({r['table']}.{r['column']})",
            f"- **What:** {r['failed_rows']:,} of {r['total_rows']:,} rows failed. Sample values: {samples}",
            f"- **Probable cause:** {cause}",
            f"- **Fix:** {action}",
            f"- **Investigate with:** `{snippet.format(col=r['column'])}`",
        ]
    if any(r["type"] == "referential" for r in failing) and ctx.get("quarantined_dims"):
        out += ["", "## Cascading effects",
                "Some dimension rows were quarantined (" + ", ".join(ctx["quarantined_dims"]) +
                "). Fact rows that reference them now fail the referential checks. "
                "Fixing the dimension defects will release part of the quarantined orders."]
    return "\n".join(out)


def anomaly_explanation(ctx: dict[str, Any]) -> str:
    anomalies = ctx.get("anomalies", [])
    if not anomalies:
        return "No anomalies detected in the selected window. Revenue moved within its normal range."
    out = []
    for a in anomalies:
        direction = "spike" if a["value"] > a["expected"] else "drop"
        pct = (a["value"] / a["expected"] - 1) * 100 if a["expected"] else 0
        out += [f"### {a['date']} - revenue {direction} ({pct:+.0f}%) - severity: {a.get('severity', 'medium')}",
                f"Revenue was **{a['value']:,.0f}** vs an expected **{a['expected']:,.0f}** "
                f"(robust z-score {a['z_score']:+.1f})."]
        breakdown = a.get("category_breakdown", [])
        total_base = sum(b["baseline"] for b in breakdown) or 1
        # ignore tiny categories - their ratios are noisy
        major = [b for b in breakdown if b["baseline"] / total_base >= 0.05]
        if major:
            overall = a["value"] / a["expected"] if a["expected"] else 1
            uniform = all(abs(b["ratio"] / overall - 1) < 0.6 for b in major if overall)
            if uniform:
                out.append("The change is spread evenly across all categories, which points to a "
                           "platform-wide event rather than a single product.")
            else:
                top = max(major, key=lambda b: abs(b["ratio"] - overall))
                out.append(f"The change is concentrated in **{top['category']}** "
                           f"({top['ratio']:.1f}x its baseline).")
        if direction == "spike":
            out.append("**Likely causes:** a sale/campaign, a bulk B2B order, or duplicate ingestion. "
                       "**Check next:** campaign calendar, top customers that day, duplicate order ids in bronze.")
        else:
            out.append("**Likely causes:** checkout or payment outage, a missing source file, or a late "
                       "batch. **Check next:** file count in the landing zone for that day, payment "
                       "gateway status, and quarantine volume for the day.")
        if a.get("severity") == "medium":
            out.append("_Borderline signal: likely normal variance from high-value orders; "
                       "monitor rather than escalate._")
        out.append("")
    return "\n".join(out).strip()


def schema_drift(ctx: dict[str, Any]) -> str:
    import difflib

    changes = ctx.get("changes", [])
    if not changes:
        return "No schema drift detected: every source file matched its contract."
    out = []
    for c in changes:
        contract = ctx.get("contracts", {}).get(c["source"], {})
        known = contract.get("required", []) + contract.get("optional", [])
        files = ", ".join(f"`{f}`" for f in c["files"][:3]) + (" ..." if c["file_count"] > 3 else "")
        if c["missing_required"]:
            out += [f"### {c['source']}: missing required column(s) {', '.join(c['missing_required'])}",
                    f"- **Files ({c['file_count']}):** {files} - **rejected**, nothing from them was loaded.",
                    "- **Likely cause:** a different export (partner feed or a changed extract job).",
                    "- **Action:** ask the source team to send the column, or agree a default and add a "
                    "mapping before re-landing the file. The rest of the batch was unaffected.", ""]
        for col in c["new_columns"]:
            close = difflib.get_close_matches(col, known, n=1, cutoff=0.6)
            hint = (f"It looks like a rename of `{close[0]}`: map it in silver instead of adding it."
                    if close else "It does not match any known column, so it is probably a new attribute.")
            out += [f"### {c['source']}: new column `{col}`",
                    f"- **Files ({c['file_count']}):** {files} - **ingested**; the column is kept in bronze "
                    "(schema evolution) but not used downstream yet.",
                    f"- **Assessment:** {hint}",
                    f"- **Action:** if it is needed, add `{col}` to the `{c['source']}` contract (optional) "
                    "and select it in the silver cleaner; otherwise acknowledge and ignore.", ""]
        if c["missing_optional"] and not c["missing_required"]:
            out += [f"### {c['source']}: optional column(s) absent: {', '.join(c['missing_optional'])}",
                    f"- **Files:** {files} - ingested; the values will be NULL.", ""]
    return "\n".join(out).strip()


def _n(q: str, default: int = 10) -> int:
    m = re.search(r"\b(\d{1,4})\b", q)
    return int(m.group(1)) if m else default


REVENUE = "status IN ('placed','shipped','delivered')"


DESTRUCTIVE = ("drop", "delete", "update", "insert", "truncate", "alter", "create")


def text_to_sql(ctx: dict[str, Any]) -> str:
    q = ctx.get("question", "").lower()
    if any(re.search(rf"\b{w}\b", q) for w in DESTRUCTIVE):
        return "CANNOT_ANSWER"
    day = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", q)
    if day and any(w in q for w in ("revenue", "sales", "orders")):
        return (f"SELECT order_date, revenue, orders, customers, avg_order_value "
                f"FROM daily_revenue WHERE order_date = DATE '{day.group(1)}'")
    n = _n(q)
    if "history" in q or "changed" in q or "changes" in q:
        return ("SELECT customer_id, COUNT(*) AS versions, MIN(valid_from) AS first_seen, "
                "MAX(valid_from) AS last_change FROM customer_history GROUP BY customer_id "
                f"HAVING COUNT(*) > 1 ORDER BY versions DESC, last_change DESC LIMIT {n}")
    if "segment" in q and any(w in q for w in ("revenue", "sales")):
        return ("SELECT customer_segment, SUM(revenue) AS revenue, SUM(units) AS units "
                "FROM mart_category_daily GROUP BY customer_segment ORDER BY revenue DESC")
    if "return rate" in q or "returns" in q:
        return (f"SELECT customer_id, segment, lifetime_orders, return_rate FROM dim_customer "
                f"WHERE lifetime_orders >= 5 ORDER BY return_rate DESC LIMIT {n}")
    if "customer" in q and any(w in q for w in ("top", "best", "highest", "most")):
        return (f"SELECT customer_id, name, country, frequency, monetary, rfm_segment "
                f"FROM customer_rfm ORDER BY monetary DESC LIMIT {n}")
    if "product" in q and any(w in q for w in ("top", "best", "highest", "most")):
        return (f"SELECT product_id, product_name, category, units_sold, revenue "
                f"FROM product_performance ORDER BY revenue DESC LIMIT {n}")
    if "category" in q or "categories" in q:
        return ("SELECT category, SUM(revenue) AS revenue, SUM(units_sold) AS units "
                "FROM product_performance GROUP BY category ORDER BY revenue DESC")
    if "segment" in q or "rfm" in q:
        return ("SELECT rfm_segment, COUNT(*) AS customers, ROUND(AVG(monetary), 2) AS avg_spend "
                "FROM customer_rfm GROUP BY rfm_segment ORDER BY customers DESC")
    if "country" in q or "countries" in q:
        return (f"SELECT country, SUM(line_amount) AS revenue, COUNT(DISTINCT order_id) AS orders "
                f"FROM orders WHERE {REVENUE} GROUP BY country ORDER BY revenue DESC")
    if "payment" in q:
        return (f"SELECT payment_method, COUNT(DISTINCT order_id) AS orders, SUM(line_amount) AS revenue "
                f"FROM orders WHERE {REVENUE} GROUP BY payment_method ORDER BY revenue DESC")
    if any(w in q for w in ("cancel", "return", "status")):
        return ("SELECT status, COUNT(DISTINCT order_id) AS orders, "
                "ROUND(100.0 * COUNT(DISTINCT order_id) / SUM(COUNT(DISTINCT order_id)) OVER (), 2) AS pct "
                "FROM orders GROUP BY status ORDER BY orders DESC")
    if "month" in q:
        return ("SELECT date_trunc('month', order_date) AS month, SUM(revenue) AS revenue, "
                "SUM(orders) AS orders FROM daily_revenue GROUP BY 1 ORDER BY 1")
    if "weekday" in q or "day of week" in q or "weekend" in q:
        return ("SELECT day_of_week, ROUND(AVG(revenue), 2) AS avg_revenue FROM daily_revenue "
                "GROUP BY day_of_week ORDER BY avg_revenue DESC")
    if "average order" in q or "aov" in q:
        return "SELECT ROUND(SUM(revenue) / SUM(orders), 2) AS avg_order_value FROM daily_revenue"
    if "how many customers" in q:
        return "SELECT COUNT(*) AS customers FROM customers"
    if "total revenue" in q or q.strip() in ("revenue", "total sales"):
        return "SELECT SUM(revenue) AS total_revenue, SUM(orders) AS total_orders FROM daily_revenue"
    if any(w in q for w in ("daily", "trend", "per day", "each day", "revenue", "last 7", "last week")):
        return ("SELECT order_date, revenue, orders, revenue_7d_avg FROM daily_revenue "
                "ORDER BY order_date")
    return "CANNOT_ANSWER"   # the mock only knows common question shapes; a real LLM covers the rest


def summarize_result(ctx: dict[str, Any]) -> str:
    rows, cols = ctx.get("rows", []), ctx.get("columns", [])
    if not rows:
        return "The query returned no rows."
    first = ", ".join(f"{c} = {v}" for c, v in zip(cols, rows[0], strict=False))
    return f"The query returned {len(rows)} row(s). First row: {first}."
