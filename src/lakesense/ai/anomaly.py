"""Revenue anomaly detection + LLM root-cause narrative.

Detection is classic statistics (robust z-score on a trailing window using
median and MAD), so it is cheap, explainable and does not hallucinate.
The LLM is used only for what it is good at: explaining the anomaly in context.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from lakesense.ai import prompts
from lakesense.ai.llm import LLMClient, get_llm
from lakesense.config import Settings
from lakesense.logging_utils import get_logger
from lakesense.serving.warehouse import connect_readonly

log = get_logger(__name__)


def weekday_factors(series: pd.Series) -> pd.Series:
    """Day-of-week seasonality index (median of weekday / overall median)."""
    s = series.astype(float)
    dow = pd.to_datetime(s.index).dayofweek
    by_dow = s.groupby(dow).median() / s.median()
    return pd.Series(by_dow.reindex(dow).values, index=s.index)


def detect(series: pd.Series, window: int = 14, threshold: float = 3.5,
           seasonal: bool = True) -> pd.DataFrame:
    """Robust z-score vs. the trailing window (current point excluded).

    z = 0.6745 * (x - median) / MAD   (Iglewicz & Hoaglin modified z-score)
    With seasonal=True the series is first divided by a day-of-week index, so a
    normal busy Saturday is not flagged as a spike.
    """
    raw = series.astype(float)
    factor = weekday_factors(raw) if seasonal and len(raw) >= 14 else pd.Series(1.0, index=raw.index)
    s = raw / factor
    hist = s.shift(1).rolling(window, min_periods=max(5, window // 2))
    median = hist.median()
    mad = s.shift(1).rolling(window, min_periods=max(5, window // 2)).apply(
        lambda x: np.median(np.abs(x - np.median(x))), raw=True)
    z = 0.6745 * (s - median) / mad.replace(0, np.nan)
    return pd.DataFrame({"value": raw, "expected": median * factor, "z_score": z,
                         "is_anomaly": z.abs() > threshold})


def _category_breakdown(con: duckdb.DuckDBPyConnection, day, window: int) -> list[dict]:
    q = """
    WITH lines AS (
      SELECT o.order_date, p.category, o.line_amount
      FROM orders o JOIN products p USING (product_id)
      WHERE o.status IN ('placed','shipped','delivered')
    ),
    day AS (SELECT category, SUM(line_amount) AS rev FROM lines WHERE order_date = ? GROUP BY 1),
    base AS (
      SELECT category, SUM(line_amount) / ? AS rev FROM lines
      WHERE order_date >= CAST(? AS DATE) - INTERVAL (?) DAY AND order_date < ? GROUP BY 1
    )
    SELECT b.category, COALESCE(d.rev, 0) AS day_revenue, b.rev AS baseline,
           COALESCE(d.rev, 0) / NULLIF(b.rev, 0) AS ratio
    FROM base b LEFT JOIN day d USING (category) ORDER BY ratio DESC
    """
    df = con.execute(q, [day, window, day, window, day]).fetchdf()
    return [{"category": r.category, "day_revenue": round(float(r.day_revenue), 2),
             "baseline": round(float(r.baseline), 2), "ratio": round(float(r.ratio or 0), 2)}
            for r in df.itertuples()]


def run(settings: Settings, llm: LLMClient | None = None) -> Path:
    cfg = settings.section("anomaly")
    window, threshold = cfg.get("window_days", 14), cfg.get("z_threshold", 3.5)
    seasonal = cfg.get("weekday_seasonality", True)
    llm = llm or get_llm()
    with connect_readonly(settings) as con:
        daily = con.execute("SELECT order_date, revenue FROM daily_revenue ORDER BY order_date").fetchdf()
        scored = detect(daily.set_index("order_date")["revenue"], window, threshold, seasonal)
        flagged = scored[scored["is_anomaly"]]
        anomalies = []
        for day, row in flagged.iterrows():
            anomalies.append({
                "date": str(pd.Timestamp(day).date()),
                "value": round(row["value"], 2), "expected": round(row["expected"], 2),
                "z_score": round(row["z_score"], 2),
                "severity": "high" if abs(row["z_score"]) >= 6 else "medium",
                "category_breakdown": _category_breakdown(con, pd.Timestamp(day).date(), window),
            })

    narrative = llm.complete(prompts.ANOMALY_SYSTEM, json.dumps(anomalies, indent=2),
                             task="anomaly_explain", context={"anomalies": anomalies})
    reports = settings.paths.reports
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "anomalies.json").write_text(json.dumps(anomalies, indent=2), encoding="utf-8")
    scored.reset_index().to_csv(reports / "revenue_scored.csv", index=False)
    out = reports / "anomaly_report.md"
    out.write_text(f"# Revenue Anomaly Report\n\n_Method: robust z-score (median/MAD), "
                   f"{window}-day trailing window, weekday-adjusted={seasonal}, |z| > {threshold}. "
                   f"LLM provider: `{llm.name}`_\n\n"
                   f"{narrative}\n", encoding="utf-8")
    log.info("Detected %d anomalies -> %s", len(anomalies), out)
    return out
