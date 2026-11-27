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


def _write_csv(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    generate()
