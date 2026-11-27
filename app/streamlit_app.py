"""LakeSense AI - Streamlit front end.

    streamlit run app/streamlit_app.py

Tabs: Overview (KPIs + anomalies) | Data Quality (AI report) | Ask your data (text-to-SQL)
"""

from __future__ import annotations

import json

import altair as alt
import pandas as pd
import streamlit as st

from lakesense.cli import load_dotenv
from lakesense.config import get_settings
from lakesense.serving.warehouse import connect_readonly

st.set_page_config(page_title="LakeSense AI", page_icon="🌊", layout="wide")
load_dotenv()
settings = get_settings()
reports = settings.paths.reports

if not settings.paths.warehouse.exists():
    st.error("Warehouse not found. Run `make demo` (or `lakesense all`) first.")
    st.stop()


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    with connect_readonly(settings) as con:
        return con.execute(sql).fetchdf()


def read_json(name: str, default):
    path = reports / name
    return json.loads(path.read_text()) if path.exists() else default


st.title("🌊 LakeSense AI")
st.caption("PySpark medallion lakehouse with an AI layer for data quality, anomalies and natural-language analytics")

overview, quality, ask = st.tabs(["📈 Overview", "🧪 Data quality", "💬 Ask your data"])

with overview:
    kpi = query("SELECT SUM(revenue) rev, SUM(orders) orders, SUM(revenue)/SUM(orders) aov FROM daily_revenue")
    health = read_json("dq_summary.json", {}).get("health_score")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue", f"{kpi.rev[0]:,.0f}")
    c2.metric("Orders", f"{int(kpi.orders[0]):,}")
    c3.metric("Avg order value", f"{kpi.aov[0]:,.2f}")
    c4.metric("DQ health score", f"{health:.1f}" if health is not None else "n/a")

    daily = query("SELECT order_date, revenue, revenue_7d_avg FROM daily_revenue ORDER BY order_date")
    anomalies = pd.DataFrame(read_json("anomalies.json", []))
    base = alt.Chart(daily).encode(x=alt.X("order_date:T", title=None))
    chart = base.mark_line(color="#4C78A8").encode(y=alt.Y("revenue:Q", title="Revenue")) + \
        base.mark_line(strokeDash=[4, 4], color="#999").encode(y="revenue_7d_avg:Q")
    if not anomalies.empty:
        anomalies["date"] = pd.to_datetime(anomalies["date"])
        chart += alt.Chart(anomalies).mark_point(size=160, filled=True, color="#E45756").encode(
            x="date:T", y="value:Q", tooltip=["date:T", "value:Q", "expected:Q", "z_score:Q"])
    st.subheader("Daily revenue (red = AI-flagged anomaly)")
    st.altair_chart(chart.properties(height=320), width="stretch")

    if (reports / "anomaly_report.md").exists():
        with st.expander("AI anomaly explanation", expanded=True):
            st.markdown((reports / "anomaly_report.md").read_text())

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by category")
        cat = query("SELECT category, SUM(revenue) revenue FROM product_performance GROUP BY 1 ORDER BY 2 DESC")
        st.bar_chart(cat, x="category", y="revenue")
    with right:
        st.subheader("Customer RFM segments")
        seg = query("SELECT rfm_segment, COUNT(*) customers FROM customer_rfm GROUP BY 1 ORDER BY 2 DESC")
        st.bar_chart(seg, x="rfm_segment", y="customers")

with quality:
    results = pd.DataFrame(read_json("dq_results.json", []))
    if results.empty:
        st.info("No DQ results yet - run the silver layer.")
    else:
        st.subheader("Rule results")
        st.dataframe(results[["table", "rule", "severity", "failed_rows", "total_rows", "pass_rate"]]
                     .sort_values("failed_rows", ascending=False), width="stretch", hide_index=True)
    if (reports / "dq_report.md").exists():
        st.subheader("AI incident report")
        st.markdown((reports / "dq_report.md").read_text())
    if (reports / "schema_drift_report.md").exists():
        st.subheader("Source schema drift")
        st.markdown((reports / "schema_drift_report.md").read_text().split("\n", 3)[-1])

with ask:
    from lakesense.ai.text_to_sql import TextToSQLAgent

    @st.cache_resource(ttl=300)   # refreshed periodically so a republished warehouse is picked up
    def agent() -> TextToSQLAgent:
        return TextToSQLAgent(settings)   # queries use a fresh cursor each time (thread-safe)

    st.write("Ask a business question. The LLM writes SQL, guardrails validate it, "
             "and it runs on a **read-only** warehouse connection.")
    examples = ["Top 10 customers by spend", "Revenue by category", "Monthly revenue",
                "Revenue by country", "Cancellation and return rate by status", "RFM segment sizes",
                "Customers whose segment changed most"]
    pick = st.selectbox("Try an example", [""] + examples)
    question = st.text_input("Your question", value=pick)
    if question:
        ans = agent().ask(question)
        if ans.sql:
            st.code(ans.sql, language="sql")
        st.caption(f"{ans.attempts} attempt(s) - {ans.latency_ms} ms - provider: {agent().llm.name}")
        if ans.ok:
            st.success(ans.summary)
            st.dataframe(ans.data, width="stretch", hide_index=True)
        else:
            st.error(f"{ans.summary} ({ans.error})")
