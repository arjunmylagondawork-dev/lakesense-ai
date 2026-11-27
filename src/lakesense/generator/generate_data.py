"""Synthetic e-commerce data generator.

Produces realistic *dirty* raw files so the pipeline has real work to do:
  data/raw/customers/customers.jsonl     (JSON lines, contains updates/duplicates)
  data/raw/products/products.csv
  data/raw/orders/orders_YYYY-MM-DD.csv  (one file per day, like a daily landing zone)

Deliberately injected problems:
  * data-quality defects: nulls, negative quantities, zero prices, invalid status,
    future timestamps, orphan product ids, duplicate rows, bad emails,
    a second timestamp format
  * business anomalies: a revenue spike and a revenue drop on configured days
The generator is seeded, so every run is reproducible (handy for tests and demos).
"""

from __future__ import annotations

import csv
import json
import random
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from lakesense.config import Settings, get_settings
from lakesense.logging_utils import get_logger

log = get_logger(__name__)

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Ananya", "Vikram", "Isha", "Arjun", "Sara",
         "Liam", "Emma", "Noah", "Olivia", "Mohammed", "Fatima", "Chen", "Mei", "Lucas", "Sofia"]
LAST = ["Sharma", "Iyer", "Khan", "Patel", "Reddy", "Nair", "Smith", "Brown", "Ali", "Wang",
        "Garcia", "Silva", "Kumar", "Das", "Menon", "Joseph", "Fernandes", "Lee", "Rao", "Shah"]
COUNTRIES = ["IN", "IN", "IN", "AE", "US", "GB", "SG", "DE"]
CATEGORIES = {
    "Electronics": (80, 1500), "Books": (5, 60), "Home": (15, 400), "Fashion": (10, 250),
    "Sports": (12, 600), "Beauty": (5, 120), "Grocery": (2, 40), "Toys": (8, 150),
}
STATUSES = ["placed", "shipped", "delivered", "delivered", "delivered", "cancelled", "returned"]
PAYMENTS = ["card", "upi", "netbanking", "wallet", "cod"]


def _customers(rng: random.Random, n: int, start: date, dirty: float) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        email = f"{first}.{last}{i}@example.com"
        if rng.random() < 0.3:
            email = email.upper()            # cosmetic - silver normalises case
        signup = start - timedelta(days=rng.randint(0, 900))
        row = {
            "customer_id": f"C{i:05d}",
            "name": f"{first} {last}",
            "email": email,
            "country": rng.choice(COUNTRIES),
            "segment": rng.choices(["consumer", "smb", "enterprise"], [0.8, 0.15, 0.05])[0],
            "signup_date": signup.isoformat(),
            "updated_at": f"{signup.isoformat()}T00:00:00",
        }
        if rng.random() < dirty:
            defect = rng.choice(["bad_email", "bad_segment", "future_signup", "null_id"])
            if defect == "bad_email":
                row["email"] = row["email"].replace("@", "_at_")
            elif defect == "bad_segment":
                row["segment"] = "unknown"
            elif defect == "future_signup":
                row["signup_date"] = "2099-01-01"
            else:
                row["customer_id"] = None
        rows.append(row)
        # ~3% of customers get a later CDC-style update record (tests dedup logic)
        if rng.random() < 0.03 and row["customer_id"]:
            upd = dict(row, segment="smb", updated_at=f"{start.isoformat()}T09:00:00")
            rows.append(upd)
    return rows


def _products(rng: random.Random, n: int, dirty: float) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        cat = rng.choice(list(CATEGORIES))
        lo, hi = CATEGORIES[cat]
        price = round(rng.uniform(lo, hi), 2)
        if rng.random() < dirty:
            price = -price
        rows.append({"product_id": f"P{i:04d}", "product_name": f"{cat} item {i}",
                     "category": cat, "list_price": price})
    return rows


def _order_lines_for_day(rng, day, n_lines, customers, products, dirty, counter):
    lines = []
    valid_customers = [c for c in customers if c["customer_id"]]
    while len(lines) < n_lines:
        counter[0] += 1
        order_id = f"O{counter[0]:07d}"
        cust = rng.choice(valid_customers)
        ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        status = rng.choice(STATUSES)
        payment = rng.choice(PAYMENTS)
        for ln in range(1, rng.choice([1, 1, 2, 2, 3, 4]) + 1):
            prod = rng.choice(products)
            row = {
                "order_line_id": f"{order_id}-{ln}",
                "order_id": order_id,
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "quantity": rng.choices([1, 2, 3, 5], [0.7, 0.18, 0.08, 0.04])[0],
                "unit_price": round(abs(prod["list_price"]) * rng.uniform(0.8, 1.0), 2),
                "status": status,
                "payment_method": payment,
                "country": cust["country"],
            }
            if rng.random() < 0.01:   # legacy source system uses another timestamp format
                row["order_ts"] = ts.strftime("%d/%m/%Y %H:%M")
            duplicate = False
            if rng.random() < dirty:
                defect = rng.choice(["null_customer", "neg_qty", "zero_price", "bad_status",
                                     "future_ts", "orphan_product", "duplicate", "bad_qty_text"])
                if defect == "null_customer":
                    row["customer_id"] = ""
                elif defect == "neg_qty":
                    row["quantity"] = -row["quantity"]
                elif defect == "zero_price":
                    row["unit_price"] = 0
                elif defect == "bad_status":
                    row["status"] = "SHIPPED??"
                elif defect == "future_ts":
                    row["order_ts"] = "2099-12-31 23:59:59"
                elif defect == "orphan_product":
                    row["product_id"] = "P9999"
                elif defect == "bad_qty_text":
                    row["quantity"] = "two"          # type error - ANSI-safe parsing must handle it
                else:
                    duplicate = True
            lines.append(row)
            if duplicate:
                lines.append(dict(row))
    return lines


def generate(settings: Settings | None = None, clean: bool = True) -> Path:
    settings = settings or get_settings()
    g = settings.section("generator")
    rng = random.Random(g.get("seed", 42))
    raw = settings.paths.raw
    if clean and raw.exists():
        shutil.rmtree(raw)
    for sub in ("customers", "products", "orders"):
        (raw / sub).mkdir(parents=True, exist_ok=True)

    start = date.fromisoformat(g.get("start_date", "2026-06-01"))
    dirty = g.get("dirty_rate", 0.03)

    customers = _customers(rng, g.get("n_customers", 2000), start, dirty)
    with open(raw / "customers" / "customers.jsonl", "w", encoding="utf-8") as fh:
        for row in customers:
            fh.write(json.dumps(row) + "\n")

    products = _products(rng, g.get("n_products", 150), dirty)
    _write_csv(raw / "products" / "products.csv", products)

    anomalies = {a["day_offset"]: a for a in g.get("anomalies", [])}
    counter = [0]
    total = 0
    for offset in range(g.get("days", 120)):
        day = start + timedelta(days=offset)
        base = g.get("avg_lines_per_day", 400)
        trend = 1 + offset * 0.002                       # gentle growth
        weekend = 1.25 if day.weekday() >= 5 else 1.0    # weekly seasonality
        noise = rng.uniform(0.9, 1.1)
        n = int(base * trend * weekend * noise)
        if offset in anomalies:
            n = max(1, int(n * anomalies[offset]["factor"]))
        lines = _order_lines_for_day(rng, day, n, customers, products, dirty, counter)
        _write_csv(raw / "orders" / f"orders_{day.isoformat()}.csv", lines)
        total += len(lines)

    log.info("Generated %d customers, %d products, %d order lines into %s",
             len(customers), len(products), total, raw)
    return raw


DRIFT_MODES = ("none", "new-column", "missing-column")


def append_days(settings: Settings | None = None, days: int = 7, drift: str = "none") -> list[Path]:
    """Land the next `days` daily files after the last existing one (incremental demo).

    Each new day also contains:
      * CDC customer updates (segment/country changes) -> exercises SCD Type 2
      * late corrections: earlier order lines re-sent with a new status -> exercises MERGE
      * optional schema drift: an extra `promo_code` column, or a partner file that
        is missing `unit_price` (rejected by the contract check)
    """
    if drift not in DRIFT_MODES:
        raise ValueError(f"drift must be one of {DRIFT_MODES}")
    settings = settings or get_settings()
    g = settings.section("generator")
    raw = settings.paths.raw
    order_files = sorted((raw / "orders").glob("orders_*.csv"))
    if not order_files:
        raise FileNotFoundError("No existing data - run `lakesense generate` first")
    rng = random.Random(g.get("seed", 42) + len(order_files))
    dirty = g.get("dirty_rate", 0.03)

    latest: dict[str, dict] = {}
    for f in sorted((raw / "customers").glob("*.jsonl")):
        for line in f.read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            if row.get("customer_id"):
                latest[row["customer_id"]] = row
    customers = list(latest.values())
    with open(raw / "products" / "products.csv", encoding="utf-8") as fh:
        products = [dict(r, list_price=float(r["list_price"])) for r in csv.DictReader(fh)]

    # continue order numbering and dates from the existing files
    max_id = 0
    for f in order_files[-3:]:
        with open(f, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                max_id = max(max_id, int(r["order_id"][1:]))
    counter = [max_id]
    last_day = date.fromisoformat(order_files[-1].stem.removeprefix("orders_")[:10])
    recent = []
    for f in order_files[-7:]:
        with open(f, encoding="utf-8") as fh:
            recent += [r for r in csv.DictReader(fh) if r["status"] in ("placed", "shipped")]

    written: list[Path] = []
    for i in range(1, days + 1):
        day = last_day + timedelta(days=i)
        n = int(g.get("avg_lines_per_day", 400) * rng.uniform(0.9, 1.1) * (1.25 if day.weekday() >= 5 else 1))
        lines = _order_lines_for_day(rng, day, n, customers, products, dirty, counter)
        # late-arriving corrections for earlier orders (same order_line_id, new status)
        base_cols = list(lines[0].keys())   # corrections must match today's file layout
        for r in rng.sample(recent, k=min(len(recent), max(1, len(recent) // 100))):
            fixed = {k: r.get(k, "") for k in base_cols}
            lines.append(dict(fixed, status=rng.choice(["delivered", "delivered", "returned"])))
        if drift == "new-column":
            for r in lines:
                r["promo_code"] = rng.choice(["", "", "", "FESTIVE10", "NEWUSER", "FLASH25"])
        path = raw / "orders" / f"orders_{day.isoformat()}.csv"
        _write_csv(path, lines)
        written.append(path)

        changes = []
        for c in rng.sample(customers, k=max(1, len(customers) // 50)):
            upd = dict(c, updated_at=f"{day.isoformat()}T10:00:00")
            if rng.random() < 0.7:
                upd["segment"] = rng.choice([s for s in ("consumer", "smb", "enterprise") if s != c["segment"]])
            else:
                upd["country"] = rng.choice([x for x in COUNTRIES if x != c["country"]])
            changes.append(upd)
        cpath = raw / "customers" / f"customers_{day.isoformat()}.jsonl"
        cpath.write_text("".join(json.dumps(r) + "\n" for r in changes), encoding="utf-8")
        written.append(cpath)

    if drift == "missing-column":
        partner = [{k: v for k, v in r.items() if k != "unit_price"} for r in lines[:50]]
        path = raw / "orders" / f"orders_{day.isoformat()}_partner.csv"
        _write_csv(path, partner)
        written.append(path)
    log.info("Appended %d day(s) through %s (%d files, drift=%s)", days, day, len(written), drift)
    return written


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    generate()
