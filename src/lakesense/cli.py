"""Command-line entry point.

    lakesense generate [--append-days N] [--drift MODE]   # synthetic raw data
    lakesense run [--layer LAYER] [--mode incremental|full]
    lakesense history silver.orders [--version N]        # Delta time travel
    lakesense maintain [--vacuum]                        # Delta OPTIMIZE / Z-ORDER
    lakesense dq-report                 # AI data-quality incident report
    lakesense anomalies                 # revenue anomaly detection + narrative
    lakesense drift-report              # AI explanation of source schema drift
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


def cmd_generate(args) -> None:
    from lakesense.generator.generate_data import append_days, generate

    if getattr(args, "append_days", 0):
        append_days(get_settings(), args.append_days, args.drift)
    else:
        generate(get_settings())


def cmd_run(args) -> None:
    settings = get_settings()
    layers = LAYERS if args.layer == "all" else [args.layer]
    full = getattr(args, "mode", "incremental") == "full"
    spark = None
    if any(layer != "publish" for layer in layers):
        from lakesense.spark_session import get_spark

        spark = get_spark(settings)
    for layer in layers:
        t0 = time.perf_counter()
        if layer == "bronze":
            from lakesense.pipeline.bronze import run_bronze
            run_bronze(spark, settings, full=full)
        elif layer == "silver":
            from lakesense.pipeline.silver import run_silver
            run_silver(spark, settings, full=full)
        elif layer == "gold":
            from lakesense.pipeline.gold import run_gold
            run_gold(spark, settings)
        elif layer == "publish":
            from lakesense.serving.warehouse import publish
            publish(settings)
        log.info("layer %-8s done in %.1fs", layer, time.perf_counter() - t0)
    if spark is not None:
        spark.stop()


def _split_table(name: str) -> tuple[str, str]:
    layer, _, table = name.partition(".")
    if not table:
        raise SystemExit("Use LAYER.TABLE, e.g. silver.orders")
    return layer, table


def _require_delta_format(settings) -> None:
    from lakesense.storage import table_format

    if table_format(settings) != "delta":
        raise SystemExit("This command needs the Delta table format "
                         "(storage.format: delta, or LAKESENSE_TABLE_FORMAT=delta).")


def cmd_history(args) -> None:
    """Delta time travel: list table versions, or show a table as of a version."""
    from lakesense.spark_session import get_spark
    from lakesense.storage import Lake

    settings = get_settings()
    _require_delta_format(settings)
    spark = get_spark(settings)
    lake = Lake(spark, settings)
    layer, table = _split_table(args.table)
    if args.version is None:
        lake.history(layer, table).show(truncate=80)
    else:
        df = lake.read(layer, table, version=args.version)
        print(f"{args.table} as of version {args.version}: {df.count()} rows")
        df.show(10, truncate=40)
    spark.stop()


def cmd_maintain(args) -> None:
    """Delta maintenance: compact small files (Z-ORDER the big fact table) and VACUUM."""
    from lakesense.spark_session import get_spark
    from lakesense.storage import Lake

    settings = get_settings()
    _require_delta_format(settings)
    spark = get_spark(settings)
    lake = Lake(spark, settings)
    lake.optimize("silver", "orders", zorder_by=["customer_id"])
    for layer, table in [("silver", "customers"), ("silver", "products"), ("bronze", "orders")]:
        lake.optimize(layer, table)
    if args.vacuum:
        for layer, table in [("bronze", "orders"), ("silver", "orders")]:
            lake.vacuum(layer, table, args.retain_hours)
    log.info("maintenance done (vacuum=%s)", args.vacuum)
    spark.stop()


def cmd_dq(_args) -> None:
    from lakesense.ai.dq_explainer import explain

    print(explain(get_settings()).read_text(encoding="utf-8"))


def cmd_drift(_args) -> None:
    from lakesense.ai.schema_drift import explain

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
    args.append_days = 0
    cmd_generate(args)
    args.layer, args.mode = "all", "full"
    cmd_run(args)
    cmd_dq(args)
    cmd_anomalies(args)
    cmd_drift(args)


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
    gen = sub.add_parser("generate", help="generate synthetic raw data")
    gen.add_argument("--append-days", type=int, default=0,
                     help="append N new days after the existing data (incremental demo)")
    gen.add_argument("--drift", choices=["none", "new-column", "missing-column"], default="none",
                     help="with --append-days: inject source schema drift")
    gen.set_defaults(func=cmd_generate)
    run = sub.add_parser("run", help="run pipeline layer(s)")
    run.add_argument("--layer", choices=LAYERS + ["all"], default="all")
    run.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                     help="incremental: only new files/batches (default); full: rebuild from all raw files")
    run.set_defaults(func=cmd_run)
    hist = sub.add_parser("history", help="Delta table history / time travel")
    hist.add_argument("table", help="LAYER.TABLE, e.g. silver.orders")
    hist.add_argument("--version", type=int, default=None, help="show the table as of this version")
    hist.set_defaults(func=cmd_history)
    mnt = sub.add_parser("maintain", help="Delta OPTIMIZE / Z-ORDER (and optional VACUUM)")
    mnt.add_argument("--vacuum", action="store_true")
    mnt.add_argument("--retain-hours", type=int, default=168)
    mnt.set_defaults(func=cmd_maintain)
    sub.add_parser("dq-report", help="AI data-quality report").set_defaults(func=cmd_dq)
    sub.add_parser("anomalies", help="revenue anomaly report").set_defaults(func=cmd_anomalies)
    sub.add_parser("drift-report", help="AI schema-drift report").set_defaults(func=cmd_drift)
    ask = sub.add_parser("ask", help="ask a question in plain English")
    ask.add_argument("question", nargs="+")
    ask.set_defaults(func=cmd_ask)
    sub.add_parser("all", help="run everything end to end").set_defaults(func=cmd_all)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
