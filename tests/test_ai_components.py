import duckdb
import numpy as np
import pandas as pd
import pytest

from lakesense.ai.anomaly import detect
from lakesense.ai.dq_explainer import health_score
from lakesense.ai.llm import LLMClient, MockLLM, get_llm
from lakesense.ai.text_to_sql import TextToSQLAgent


def _series(n=60, seed=1):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2026-01-01", periods=n, freq="D")
    weekend = np.where(idx.dayofweek >= 5, 1.25, 1.0)
    return pd.Series(1000 * weekend * rng.uniform(0.95, 1.05, n), index=idx)


def test_detect_finds_injected_spike_and_drop():
    s = _series()
    s.iloc[40] *= 4
    s.iloc[50] *= 0.1
    flagged = set(np.where(detect(s)["is_anomaly"])[0])
    assert {40, 50} <= flagged


def test_detect_ignores_normal_weekends():
    assert not detect(_series(), seasonal=True)["is_anomaly"].any()


def test_health_score_weights_errors_more():
    def r(rate, sev):
        return {"table": "t", "pass_rate": rate, "severity": sev}

    errs = [r(0.9, "error"), r(1.0, "warn")]
    warns = [r(1.0, "error"), r(0.9, "warn")]
    assert health_score(errs) < health_score(warns)
    assert health_score(errs) == 90.0
    assert health_score([]) == 100.0


def test_get_llm_defaults_and_validation(monkeypatch):
    monkeypatch.delenv("LAKESENSE_LLM_PROVIDER", raising=False)
    assert isinstance(get_llm(), MockLLM)
    with pytest.raises(ValueError):
        get_llm("not-a-provider")


class FlakyLLM(LLMClient):
    """Returns broken SQL first, then a fix - exercises the self-correction loop."""
    name = "flaky"

    def __init__(self):
        self.calls = 0

    def complete(self, system, prompt, *, task="", context=None):
        if task == "summarize_result":
            return "ok"
        self.calls += 1
        return "SELECT revenu FROM daily_revenue" if self.calls == 1 else "SELECT revenue FROM daily_revenue"


class MalformedThenFixedLLM(LLMClient):
    """First answer does not even parse; the agent must send the parser error back and retry."""
    name = "malformed"

    def __init__(self):
        self.prompts = []

    def complete(self, system, prompt, *, task="", context=None):
        if task == "summarize_result":
            return "ok"
        self.prompts.append(prompt)
        if len(self.prompts) == 1:
            return "SELECT 'unterminated FROM daily_revenue"
        return "SELECT revenue FROM daily_revenue"


class UnanswerableLLM(LLMClient):
    name = "unanswerable"

    def complete(self, system, prompt, *, task="", context=None):
        return "CANNOT_ANSWER"


class EvilLLM(LLMClient):
    name = "evil"

    def complete(self, system, prompt, *, task="", context=None):
        return "DROP TABLE daily_revenue"


@pytest.fixture()
def mem_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE daily_revenue AS SELECT DATE '2026-01-01' AS order_date, 10.0 AS revenue")
    yield con
    con.close()


def test_text_to_sql_self_corrects(small_settings, mem_con):
    llm = FlakyLLM()
    ans = TextToSQLAgent(small_settings, llm=llm, con=mem_con).ask("revenue?")
    assert ans.ok and ans.attempts == 2 and len(ans.data) == 1
    assert [h["status"] for h in ans.history] == ["error", "ok"]


def test_text_to_sql_blocks_destructive_output(small_settings, mem_con):
    ans = TextToSQLAgent(small_settings, llm=EvilLLM(), con=mem_con).ask("delete everything")
    assert not ans.ok and ans.error.startswith("Blocked")
    assert mem_con.execute("SELECT count(*) FROM daily_revenue").fetchone()[0] == 1


def test_text_to_sql_retries_malformed_sql(small_settings, mem_con):
    llm = MalformedThenFixedLLM()
    ans = TextToSQLAgent(small_settings, llm=llm, con=mem_con).ask("revenue?")
    assert ans.ok and ans.attempts == 2
    assert [h["status"] for h in ans.history] == ["invalid", "ok"]
    assert "does not parse" in llm.prompts[1]          # error was fed back to the model


def test_text_to_sql_unanswerable(small_settings, mem_con):
    ans = TextToSQLAgent(small_settings, llm=UnanswerableLLM(), con=mem_con).ask("what is the weather?")
    assert not ans.ok and ans.sql is None and ans.attempts == 1
