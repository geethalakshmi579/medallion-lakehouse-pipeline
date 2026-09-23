#!/usr/bin/env python3
"""Generate synthetic raw data for the medallion pipeline demo.

Writes:
  data/raw/orders/*.json        - raw order events (JSON lines), with injected issues
  data/raw/customers/customers.csv - customer master snapshot

Deterministic (seed=42). All data is fictional.
Injected issues: duplicate events, negative quantities, missing customer ids,
a late-arriving schema change (a `promo_code` field appears mid-stream).
"""
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
BASE = Path(__file__).resolve().parent
RAW = BASE / "raw"

random.seed(SEED)

PRODUCTS = [
    ("P01", "Electronics", 129.99), ("P02", "Home", 34.50),
    ("P03", "Electronics", 79.00), ("P04", "Apparel", 59.99),
    ("P05", "Home", 45.00), ("P06", "Apparel", 89.95),
]
CHANNELS = ["web", "mobile", "store"]


def main(n_orders=3000, n_customers=500):
    for d in [RAW / "orders", RAW / "customers"]:
        d.mkdir(parents=True, exist_ok=True)

    customers = [
        {"customer_id": f"C{i:04d}",
         "name": f"Customer {i}",
         "email": f"customer{i}@example.com",
         "country": random.choice(["US", "US", "US", "IN", "UK"]),
         "signup_date": str(date(2025, 1, 1) + timedelta(days=random.randint(0, 200)))}
        for i in range(1, n_customers + 1)
    ]
    with open(RAW / "customers" / "customers_day1.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
        w.writeheader()
        w.writerows(customers)

    # day-2 snapshot: 10% of customers change country/email (drives the SCD2 demo)
    day2 = []
    for c in customers:
        c2 = dict(c)
        if random.random() < 0.10:
            c2["country"] = random.choice(["US", "IN", "UK", "CA"])
            c2["email"] = f"new-{c2['email']}"
        day2.append(c2)
    with open(RAW / "customers" / "customers_day2.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(day2[0].keys()))
        w.writeheader()
        w.writerows(day2)

    start = date(2026, 1, 1)
    events = []
    for i in range(1, n_orders + 1):
        c = random.choice(customers)
        pid, cat, price = random.choice(PRODUCTS)
        evt = {
            "order_id": f"O{i:05d}",
            "customer_id": c["customer_id"],
            "product_id": pid,
            "category": cat,
            "quantity": random.randint(1, 3),
            "unit_price": price,
            "order_ts": str(start + timedelta(days=random.randint(0, 60),
                                              seconds=random.randint(0, 86399))),
            "channel": random.choice(CHANNELS),
        }
        # schema drift: promo_code appears halfway through the stream
        if i > n_orders // 2:
            evt["promo_code"] = random.choice(["SAVE10", "WELCOME", None])
        events.append(evt)

    # inject issues: 2% duplicates, 1% negative qty, 1% unknown customer
    n = len(events)
    for idx in random.sample(range(n), n // 50):
        events.append(dict(events[idx]))                       # exact duplicate event
    for idx in random.sample(range(n), n // 100):
        events[idx] = dict(events[idx], quantity=-1)            # bad quantity
    for idx in random.sample(range(n), n // 100):
        events[idx] = dict(events[idx], customer_id="C9999")   # orphan customer

    random.shuffle(events)
    out = RAW / "orders" / "orders.json"
    with open(out, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"wrote {out} ({len(events)} events), customers_day1/2.csv ({len(customers)} rows)")


if __name__ == "__main__":
    main()
