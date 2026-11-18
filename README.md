# Databricks Medallion Assignment

This repository contains a reproducible implementation of a Bronze -> Silver -> Gold e-commerce lakehouse.

## Submission status

The code, notebooks, SQL, and job configuration are implementation-ready. Live Databricks account evidence, workspace screenshots, job-run screenshots, AI/BI screenshots, and a Power BI `.pbix` require access to the candidate's own Databricks trial workspace and must be captured there. This repository does not fabricate account or run evidence.

## Architecture

- **Sources:** reproducibly generated `orders.csv` and `transactions.csv`
- **Landing:** Unity Catalog volume `/Volumes/<catalog>/ecom_poc/raw_files/`
- **Bronze:** source fields as STRING plus `_ingested_at`, `_source_file`, `_batch_id`, and `_row_hash`
- **Silver:** typed, validated, deterministic deduplication; separate quarantine and duplicate-loser tables
- **Gold:** four documented analytics marts
- **Consumption:** Databricks AI/BI, SQL/PBI validation; Power BI native or exported-Gold fallback

## Required catalog setup

Replace `<catalog>` with a catalog where you have `CREATE SCHEMA` privilege:

```sql
USE CATALOG <catalog>;
CREATE SCHEMA IF NOT EXISTS ecom_poc;
USE SCHEMA ecom_poc;
CREATE VOLUME IF NOT EXISTS raw_files;
```

The default raw path is `/Volumes/<catalog>/ecom_poc/raw_files/`.

## Generate and upload source files

```powershell
python generator\generate_synthetic_data.py
```

The script generates exactly 1,000,000 data rows in each CSV using seed `20261119`. It writes:

- `generator/output/orders.csv`
- `generator/output/transactions.csv`
- `generator/output/dirty_data_inventory.json`
- `generator/output/dirty_data_inventory.csv`

Upload only the two CSVs to the managed volume. Do not commit them; they are intentionally excluded by `.gitignore`.

## Databricks execution

1. Import or develop `notebooks/setup_and_environment.py` in a Git folder.
2. Set `CATALOG`, `SCHEMA`, and `BATCH_ID` widget values.
3. Run setup, Bronze, Silver, and Gold in order.
4. Run `sql/quality_assertions.sql` and `sql/gold_metrics.sql`.
5. Import `operations/job.json` as a Lakeflow Job and configure a SQL/interactive serverless environment.

The notebooks use serverless SQL/Python operations where available. The expected row invariant is:

```text
bronze_orders = silver_orders + quarantine_orders + duplicate_orders
bronze_transactions = silver_transactions + quarantine_transactions + duplicate_transactions
```

No row can appear in more than one outcome bucket.

## Silver processing precedence

For each dataset:

1. Perform safe type conversion.
2. Apply mandatory-field and business-rule validation.
3. Put every failed record into the quarantine table with pipe-separated rejection reasons.
4. Among valid records only, deduplicate the business key.
5. Store removed valid-key duplicates in the duplicate-loser table.
6. Keep one deterministic winner.

For orders, the winner is the valid row with the lexicographically smallest `_row_hash`. For transactions, the same rule applies to `txn_id`. Exact duplicate rows are therefore key duplicates, but a separate loser table preserves both exact-row and non-exact-row duplicate evidence. `_row_hash` is identical for exact duplicated source values.

## Revenue definition

Source of truth: `silver_transactions` joined once to `silver_orders` by `order_id`.

**Revenue = gross successful USD transaction amount for a transaction whose order has `order_status = completed` and transaction status `success`.**

- Currency scope: USD only. No invented foreign-exchange rate is used.
- Refunds/cancellations: excluded.
- Repeat/partial payments: each accepted successful `txn_id` is a separate payment event. No revenue is multiplied by a join to a unique order.
- USD is normalized directly without rounding during aggregation; report and dashboard display values to two decimal places.
- If a report needs another currency or includes cancellations/refunds, it is not directly comparable without an explicitly documented FX/refund rule.

Small example:

| order_id | order status | txn_id | txn status | currency | amount | Revenue |
|---|---|---|---|---:|---:|---:|
| O1 | completed | T1 | success | USD | 80.00 | 80.00 |
| O1 | completed | T2 | success | USD | 20.00 | 20.00 |
| O1 | cancelled | T3 | success | USD | 100.00 | 0.00 |
| O2 | completed | T4 | refund | USD | 15.00 | 0.00 |

Total comparable revenue: 100.00 USD.

## Gold output grains

| Table | Grain | Unique key |
|---|---|---|
| `gold_daily_sales` | one row per order date | `sales_date` |
| `gold_revenue_breakdown` | category and country | `category`, `country` |
| `gold_payment_success` | payment method | `payment_method` |
| `gold_top_customers` | customer | `customer_id` |

## Data security

Synthetic CSVs contain no real customer data, credentials, tokens, or secrets and must remain outside Git. In production:

- Restrict source volume writes and table writes by Unity Catalog roles.
- Use managed secrets/identity-based access; never embed credentials.
- Scan uploads for PII and malware.
- Encrypt at rest and in transit.
- Retain source lineage and retention/audit metadata.
- Add file approval and schema checks before Bronze promotion.
- Grant downstream reporting users Gold-only `USE CATALOG`, `USE SCHEMA`, and `SELECT` privileges.

## Evidence checklist

- `[ ]` Own-email Databricks trial workspace
- `[ ]` Catalog, schema, volume setup
- `[ ]` Source row-count and byte-size evidence
- `[ ]` Bronze schema and reconciliation
- `[ ]` Silver quarantine and duplicate evidence
- `[ ]` Gold output counts and samples
- `[ ]` AI/BI dashboard and three verified natural-language prompts
- `[ ]` Power BI report and total-revenue comparison
- `[ ]` Lakeflow successful run
- `[ ]` Least-privilege grants and cleanup

