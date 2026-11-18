from __future__ import annotations

import csv
import json
import random
from collections import Counter
from pathlib import Path

SEED = 20261119
ROW_COUNT = 1_000_000
OUTPUT_DIR = Path(__file__).resolve().parent / "output"

random.seed(SEED)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def increment(counter: Counter, category: str, field: str) -> None:
    counter[f"{category}|{field}"] += 1


def generate_orders() -> Counter:
    categories = ["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books"]
    countries = ["USA", "UK", "Germany", "Canada", "France", "Japan"]
    statuses = ["completed", "cancelled", "pending", "refunded"]
    anomalies: Counter = Counter()
    source_rows: dict[int, list[str]] = {}

    with (OUTPUT_DIR / "orders.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["order_id", "customer_id", "order_date", "category", "quantity", "unit_price", "order_status", "country"])

        for i in range(1, ROW_COUNT + 1):
            order_id = f"O{i:07d}"
            customer_id = f"C{random.randint(1, 120_000):06d}"
            order_date = f"{random.randint(2025, 2026):04d}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            category = random.choice(categories)
            quantity = str(random.randint(1, 8))
            unit_price = f"{random.uniform(5.0, 2_500.0):.2f}"
            status = random.choice(statuses)
            country = random.choice(countries)

            if i % 97 == 0:
                customer_id = ""
                increment(anomalies, "null_or_blank", "customer_id")
            if i % 101 == 0:
                quantity = "one"
                increment(anomalies, "invalid_text_type", "quantity")
            if i % 103 == 0:
                quantity = "-2"
                increment(anomalies, "negative_or_out_of_range", "quantity")
            if i % 107 == 0:
                unit_price = "not-a-price"
                increment(anomalies, "invalid_text_type", "unit_price")
            if i % 109 == 0:
                unit_price = "-25.00"
                increment(anomalies, "negative_or_out_of_range", "unit_price")
            if i % 113 == 0:
                order_date = "2028-12-31"
                increment(anomalies, "invalid_future_timestamp", "order_date")
            if i % 127 == 0:
                order_date = "2026/99/99"
                increment(anomalies, "invalid_timestamp", "order_date")
            if i % 131 == 0:
                category = f"  {category.lower()}  "
                increment(anomalies, "case_and_whitespace", "category")
            if i % 137 == 0:
                status = "UNKNOWN"
                increment(anomalies, "invalid_status", "order_status")
            if i % 139 == 0:
                country = ""
                increment(anomalies, "null_or_blank", "country")

            # Produce 2% duplicate business keys, including 0.2% exact duplicate rows.
            if i > ROW_COUNT - 20_000:
                source_i = random.randint(1, ROW_COUNT - 20_000)
                order_id = f"O{source_i:07d}"
                if i <= ROW_COUNT - 18_000:
                    row = source_rows[source_i]
                    order_id, customer_id, order_date, category, quantity, unit_price, status, country = row
                    increment(anomalies, "duplicate_exact_or_key", "order_id")
                else:
                    increment(anomalies, "duplicate_key", "order_id")

            row = [order_id, customer_id, order_date, category, quantity, unit_price, status, country]
            if i <= ROW_COUNT - 20_000:
                source_rows[i] = row.copy()
            writer.writerow(row)

    return anomalies


def generate_transactions() -> Counter:
    payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash"]
    currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
    statuses = ["success", "failed", "pending", "refund"]
    anomalies: Counter = Counter()
    source_rows: dict[int, list[str]] = {}

    with (OUTPUT_DIR / "transactions.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txn_id", "order_id", "payment_method", "amount", "currency", "txn_status", "txn_ts"])

        for i in range(1, ROW_COUNT + 1):
            txn_id = f"T{i:07d}"
            order_id = f"O{random.randint(1, ROW_COUNT - 20_000):07d}"
            payment_method = random.choice(payment_methods)
            amount = f"{random.uniform(5.0, 2_500.0):.2f}"
            currency = random.choice(currencies)
            txn_status = random.choice(statuses)
            txn_ts = f"{random.randint(2025, 2026):04d}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"

            if i % 83 == 0:
                txn_id = ""
                increment(anomalies, "null_or_blank", "txn_id")
            if i % 89 == 0:
                amount = "twenty"
                increment(anomalies, "invalid_text_type", "amount")
            if i % 97 == 0:
                amount = "-10.00"
                increment(anomalies, "negative_or_out_of_range", "amount")
            if i % 107 == 0:
                currency = "US D"
                increment(anomalies, "unsupported_currency", "currency")
            if i % 113 == 0:
                payment_method = " crypto "
                increment(anomalies, "invalid_status_or_label", "payment_method")
            if i % 127 == 0:
                txn_status = "SUCCESS "
                increment(anomalies, "case_and_whitespace", "txn_status")
            if i % 131 == 0:
                txn_ts = "2028-12-31 23:59:59"
                increment(anomalies, "invalid_future_timestamp", "txn_ts")
            if i % 137 == 0:
                txn_ts = "bad timestamp"
                increment(anomalies, "invalid_timestamp", "txn_ts")
            if i % 139 == 0:
                order_id = f"O{ROW_COUNT + i:07d}"
                increment(anomalies, "orphan_reference", "order_id")

            if i > ROW_COUNT - 20_000:
                source_i = random.randint(1, ROW_COUNT - 20_000)
                txn_id = f"T{source_i:07d}"
                if i <= ROW_COUNT - 18_000:
                    row = source_rows[source_i]
                    txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts = row
                    increment(anomalies, "duplicate_exact_or_key", "txn_id")
                else:
                    increment(anomalies, "duplicate_key", "txn_id")

            row = [txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts]
            if i <= ROW_COUNT - 20_000:
                source_rows[i] = row.copy()
            writer.writerow(row)

    return anomalies


def row_count(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return sum(1 for _ in reader)


def inventory_rows(counter: Counter) -> list[dict]:
    return [
        {
            "category": key.split("|", 1)[0],
            "field": key.split("|", 1)[1],
            "count": count,
            "approx_percent": round(count * 100 / ROW_COUNT, 4),
        }
        for key, count in sorted(counter.items())
    ]


if __name__ == "__main__":
    order_anomalies = generate_orders()
    transaction_anomalies = generate_transactions()

    inventory = {
        "seed": SEED,
        "rows_per_file": ROW_COUNT,
        "orders": {
            "row_count": row_count(OUTPUT_DIR / "orders.csv"),
            "inventory": inventory_rows(order_anomalies),
        },
        "transactions": {
            "row_count": row_count(OUTPUT_DIR / "transactions.csv"),
            "inventory": inventory_rows(transaction_anomalies),
        },
    }

    (OUTPUT_DIR / "dirty_data_inventory.json").write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    with (OUTPUT_DIR / "dirty_data_inventory.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["file", "category", "field", "count", "approx_percent"])
        for file_name in ("orders", "transactions"):
            for item in inventory[file_name]["inventory"]:
                writer.writerow([file_name, item["category"], item["field"], item["count"], item["approx_percent"]])

    print(json.dumps(inventory, indent=2))
