"""Command-line entry point.

    lakesense generate                  # create synthetic raw data
    lakesense run [--layer LAYER]       # bronze | silver | gold | publish | all
    lakesense dq-report                 # AI data-quality incident report
    lakesense anomalies                 # revenue anomaly detection + narrative
    lakesense ask "top 5 customers"     # natural language -> SQL -> answer
    lakesense all                       # everything, end to end
"""

from __future__ import annotations

import argparse
import sys
import time

from lakesense.config import get_settings
from lakesense.logging_utils import get_logger

log = get_logger("lakesense")
LAYERS = ["bronze", "silver", "gold", "publish"]


def cmd_generate(_args) -> None:
    from lakesense.generator.generate_data import generate

    generate(get_settings())


def cmd_run(args) -> None:
    settings = get_settings()
    layers = LAYERS if args.layer == "all" else [args.layer]
    spark = None
    if any(layer != "publish" for layer in layers):
        from lakesense.spark_session import get_spark

        spark = get_spark(settings)
    for layer in layers:
        t0 = time.perf_counter()
        if layer == "bronze":
            from lakesense.pipeline.bronze import run_bronze
            run_bronze(spark, settings)
        elif layer == "silver":
            from lakesense.pipeline.silver import run_silver
            run_silver(spark, settings)
        elif layer == "gold":
            from lakesense.pipeline.gold import run_gold
            run_gold(spark, settings)
        elif layer == "publish":
            from lakesense.serving.warehouse import publish
            publish(settings)
        log.info("layer %-8s done in %.1fs", layer, time.perf_counter() - t0)
    if spark is not None:
        spark.stop()


def cmd_dq(_args) -> None:
    from lakesense.ai.dq_explainer import explain

    print(explain(get_settings()).read_text(encoding="utf-8"))


def cmd_anomalies(_args) -> None:
    from lakesense.ai.anomaly import run

    print(run(get_settings()).read_text(encoding="utf-8"))


def cmd_ask(args) -> None:
    from lakesense.ai.text_to_sql import TextToSQLAgent

    ans = TextToSQLAgent(get_settings()).ask(" ".join(args.question))
    if ans.sql:
        print(f"\nSQL ({ans.attempts} attempt(s), {ans.latency_ms} ms):\n{ans.sql}\n")
    if ans.ok:
        print(ans.data.to_string(index=False, max_rows=30))
        print(f"\n{ans.summary}")
    else:
        print(f"\n{ans.summary}\nReason: {ans.error}")
        sys.exit(1)


def cmd_all(args) -> None:
    cmd_generate(args)
    args.layer = "all"
    cmd_run(args)
    cmd_dq(args)
    cmd_anomalies(args)


def load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader: KEY=VALUE lines; real environment variables win."""
    import os
    from pathlib import Path

    env = Path(path)
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.split(" #")[0].strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="lakesense", description="AI-augmented lakehouse pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("generate", help="generate synthetic raw data").set_defaults(func=cmd_generate)
    run = sub.add_parser("run", help="run pipeline layer(s)")
    run.add_argument("--layer", choices=LAYERS + ["all"], default="all")
    run.set_defaults(func=cmd_run)
    sub.add_parser("dq-report", help="AI data-quality report").set_defaults(func=cmd_dq)
    sub.add_parser("anomalies", help="revenue anomaly report").set_defaults(func=cmd_anomalies)
    ask = sub.add_parser("ask", help="ask a question in plain English")
    ask.add_argument("question", nargs="+")
    ask.set_defaults(func=cmd_ask)
    sub.add_parser("all", help="run everything end to end").set_defaults(func=cmd_all)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
