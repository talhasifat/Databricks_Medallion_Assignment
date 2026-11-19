# Databricks Medallion Assignment: Trustworthy E-commerce Lakehouse

**Instructor:** Lipton Ahammed  |  **Submission date:** 19 November 2026

An end-to-end **Bronze → Silver → Gold** data platform on a Databricks free trial. Synthetic, reproducible order and payment data is ingested, cleaned with explicit rules, reconciled row-for-row, modelled into Gold marts, and reported through Databricks AI/BI and Power BI.

> **How to read this README.** Every number, screenshot and link in it must come from my own trial workspace. Items marked **`TODO`** are filled in from real runs and are never estimated or invented. Screenshots are stored in `evidence/` and linked from the sections below.

---

## 1. Business problem and success criteria

An e-commerce company receives daily CSV extracts of **orders** and **payment transactions**. Nulls, text in numeric fields, inconsistent labels, bad timestamps, orphan references and duplicate keys make sales reports unreliable.

This project must show:

| Success check | Where it is proven |
|---|---|
| Source row count = Bronze row count (both files) | Section 5 |
| Bronze = accepted Silver + quarantined + duplicate losers, no overlap | Section 6 |
| Every Gold table has a documented grain and key | Section 7 |
| Total revenue is identical in SQL, AI/BI and Power BI | Section 9 |
| A Lakeflow job runs Bronze → Silver → Gold successfully | Section 10 |
| Every trial limitation and fallback is disclosed | Section 12 |

## 2. Architecture

```
orders.csv + transactions.csv (synthetic, seed 20261119, 1,000,000 data rows each)
        |
        v
Unity Catalog volume   /Volumes/workspace/ecom_poc/raw_files/        (raw files, unchanged)
        |
        v
BRONZE   bronze_orders, bronze_transactions                          (all STRING + audit columns)
        |
        v
SILVER   silver_orders, silver_transactions                          (typed, validated, unique keys)
         quarantine_orders, quarantine_transactions                  (rejected rows + reasons)
         duplicate_orders, duplicate_transactions                    (deterministic duplicate losers)
        |
        v
GOLD     gold_daily_sales, gold_revenue_breakdown,
         gold_payment_success, gold_top_customers
        |
        v
Databricks SQL warehouse -> AI/BI dashboard, Genie Agent, Power BI Desktop
Orchestration: Lakeflow Job (bronze -> silver -> gold)
```

### Naming standard

| Item | Convention |
|---|---|
| Catalog / schema / volume | `workspace` / `ecom_poc` / `raw_files` |
| Layer prefix | `bronze_`, `silver_`, `gold_` (the prefix always tells the layer) |
| Rejected / duplicate outcomes | `quarantine_<dataset>`, `duplicate_<dataset>` |
| Audit columns | leading underscore: `_ingested_at`, `_source_file`, `_batch_id`, `_row_hash` |
| Columns | lower-case snake_case, source names kept exactly as in the CSV |
| Raw path | `/Volumes/workspace/ecom_poc/raw_files/` |

## 3. Repository layout

| Folder / file | Contents |
|---|---|
| `README.md` | This document, with written answers to every assignment question |
| `generator/` | Seeded generation script and dirty-data inventory (CSV/JSON). The two large CSVs are **not** committed |
| `notebooks/` | `setup_and_environment`, `bronze_ingestion`, `silver_cleaning`, `gold_models` |
| `sql/` | Setup, rules matrix, quality assertions, Gold metrics, grants, cross-tool validation |
| `dashboards/` | AI/BI notes, dashboard screenshots, Genie checks |
| `powerbi/` | `.pbix` file, report screenshot, notes |
| `evidence/` | Topic-labelled screenshots and output snippets |
| `operations/` | Job configuration, least-privilege plan, credit check, cleanup notes |

---

## 4. Trial workspace and environment

| Item | Value |
|---|---|
| Account email | Own personal email (verified) |
| Cloud / provider | AWS |
| Region | us-east-2 |
| Metastore | `metastore_aws_us_east_2` |
| Workspace name | `workspace` |
| Workspace URL | `dbc-b27142e6-4061.cloud.databricks.com` |
| Trial start | 17 November 2026 |
| Trial expiry | `TODO` (read from the billing/usage page) |
| Remaining credits at start / end | `TODO` / `TODO` |
| Catalog used | `workspace` |
| Notebook compute | Serverless (`TODO` confirm in Compute page) |
| SQL warehouse | Serverless Starter Warehouse, size `TODO`, auto-stop `TODO` |
| Account restrictions observed | `TODO` (write "none" if none) |

![Workspace home page](evidence/01_workspace_home.png)
![Trial and credit status](evidence/02_trial_credits.png)

### Trial vs Free Edition

The assignment requires a real **free trial**, not Community Edition or Free Edition. The trial is a time-limited, credit-based evaluation of a full workspace (here a 14-day window), so it can use features such as Lakeflow Jobs, SQL warehouses and Genie within the credit limit. Free Edition is a separate, permanently free offering with restricted compute and quotas. Official comparison: Databricks "free trial versus Free Edition" documentation. `TODO`: note any feature that was limited in my account.

### Schema and volume setup

```sql
USE CATALOG workspace;
CREATE SCHEMA IF NOT EXISTS ecom_poc;
USE SCHEMA ecom_poc;
CREATE VOLUME IF NOT EXISTS raw_files;
```

Saved as `sql/setup.sql`. Exact raw-file path: `/Volumes/workspace/ecom_poc/raw_files/`.

![Catalog Explorer: catalog, schema, volume](evidence/03_catalog_schema_volume.png)

### Compute choices and credit control

- **ETL:** serverless notebooks. Nothing to size or manage, and they stop when idle.
- **BI:** one small SQL warehouse, used only for dashboards, Genie and Power BI.
- **Minimizing credits:** short auto-stop on the warehouse, stop it after reporting work, no parallel experiments, no schedules left enabled, one successful job run as evidence.

![Compute and SQL warehouse](evidence/04_compute_warehouse.png)

### Git

Work is versioned in a GitHub repository connected through a Databricks Git folder, with a commit after each stage.

- Repository URL: `TODO`
- Integration limitation (if any): `TODO`

![Commit history](evidence/05_git_commits.png)

---

## 5. Synthetic source data and Bronze

### 5.1 Source files

`generator/generate_synthetic_data.py` creates both files with a **fixed seed (`20261119`)**, so a re-run produces the same data.

| File | Columns | Data rows |
|---|---|---|
| `orders.csv` | order_id, customer_id, order_date, category, quantity, unit_price, order_status, country | 1,000,000 |
| `transactions.csv` | txn_id, order_id, payment_method, amount, currency, txn_status, txn_ts | 1,000,000 |

Run: `python generator/generate_synthetic_data.py`

### 5.2 Dirty-data inventory

Source: `generator/output/dirty_data_inventory.csv`. `TODO`: paste the final table here after the last generator run (type, field, rows affected, approximate %).

| File | Dirty-data type | Field | Rows | Approx % |
|---|---|---|---:|---:|
| orders | `TODO` | `TODO` | `TODO` | `TODO` |
| transactions | `TODO` | `TODO` | `TODO` | `TODO` |

Dirty-data categories covered per file: null/blank values, text inside numeric fields, negative or out-of-range numbers, invalid and future timestamps, inconsistent case/whitespace, duplicate keys and exact duplicate rows, and (transactions) orphan order IDs and unsupported currencies.

**Why must the anomalies stay a small, measured fraction?** Real data is mostly valid with a minority of defects. If most rows were bad, the validation rules, quarantine volumes and Gold metrics would not look like a realistic pipeline, and the analytics would be meaningless. Measuring each injected fraction also lets me verify that Silver found exactly what was planted.

### 5.3 Upload and row counts

Both CSVs were uploaded through **New → Add or upload data → Upload files to a volume**.

| File | Data rows (excl. header) | Bytes | Volume path |
|---|---:|---:|---|
| orders.csv | `TODO` | `TODO` | `/Volumes/workspace/ecom_poc/raw_files/orders.csv` |
| transactions.csv | `TODO` | `TODO` | `/Volumes/workspace/ecom_poc/raw_files/transactions.csv` |

![Upload to volume](evidence/06_upload_volume.png)

### 5.4 Why raw data, credentials and extracts stay out of Git

- Git history is permanent and widely copied. A committed secret or customer record cannot be reliably removed.
- Raw extracts in real projects contain personal data, which creates privacy and legal exposure.
- Very large files bloat the repository and are not source code.

`.gitignore` excludes `*.csv`, `.env`, `.databricks/`, caches and build artefacts. (`*.pbix` is **not** ignored, because the Power BI file is a required deliverable.)

**Minimum production controls:** Unity Catalog role-based access on volumes and tables; secrets in a managed secret store or workload identity, never in code; PII scanning on uploads; encryption at rest and in transit; audit logging and retention rules; schema and file-approval checks before Bronze promotion; reporting users limited to Gold.

### 5.5 Bronze: lossless ingestion

`notebooks/bronze_ingestion.py` reads each CSV with `header=true` and **`inferSchema=false`**, keeps every source column as `STRING`, and adds audit metadata.

**Why malformed data belongs in Bronze.** Bronze is the permanent record of what arrived. If bad rows were rejected at ingestion, the evidence would be lost, counts could not be reconciled to the source, and a bug in a cleaning rule could not be fixed by reprocessing. Cleaning decisions are made in Silver, where every rejection is recorded.

| Audit column | Meaning |
|---|---|
| `_ingested_at` | Timestamp of the load, for freshness and troubleshooting |
| `_source_file` | Full volume path of the file the row came from (lineage) |
| `_batch_id` | Identifier of the load run, used for idempotent re-runs |
| `_row_hash` | SHA-256 of the source fields. Identical source rows get identical hashes, which supports duplicate detection and row tracing |

**Idempotent loading.** Each load carries a stable `_batch_id`. Before appending, the notebook deletes any existing rows with the same `_batch_id`, so re-running the same batch replaces it instead of doubling it.

**Lossless proof** (`TODO`: paste the real output of the count query):

| Table | Source data rows | Bronze rows | Equal? |
|---|---:|---:|---|
| bronze_orders | `TODO` | `TODO` | `TODO` |
| bronze_transactions | `TODO` | `TODO` | `TODO` |

![Bronze schema and sample rows](evidence/07_bronze_schema_sample.png)
![Bronze count reconciliation](evidence/08_bronze_counts.png)

---

## 6. Silver: cleaning, validation and reconciliation

### 6.1 Safe type conversion

Silver converts text to `DATE`, `TIMESTAMP`, `INT` and `DECIMAL(18,2)` with **`try_cast` / `try_to_timestamp`**. A string that cannot be parsed becomes `NULL` instead of failing the job. The mandatory-field and range rules then reject that row, so it lands in quarantine with a reason such as `invalid_quantity` or `invalid_order_date`.

![Before/after conversion examples](evidence/09_silver_conversion_examples.png)

### 6.2 Business-rule matrix

Implemented in `notebooks/silver_cleaning.py`; documented in `sql/rules_matrix.sql`.

| Dataset | Field | Rule |
|---|---|---|
| orders | order_id | Mandatory (non-blank) |
| orders | customer_id | Mandatory |
| orders | order_date | Valid `YYYY-MM-DD`, not NULL, not in the future |
| orders | category | Mandatory; normalized (trim, lower-case) |
| orders | quantity | Integer from 1 to 100 |
| orders | unit_price | Decimal greater than 0 and at most 100,000 |
| orders | order_status | `completed`, `cancelled`, `pending`, `refunded` (after trim/lower-case) |
| orders | country | Mandatory; normalized (trim, upper-case) |
| transactions | txn_id | Mandatory |
| transactions | order_id | Mandatory and present in accepted `silver_orders` (orphan check) |
| transactions | payment_method | `credit_card`, `debit_card`, `paypal`, `bank_transfer`, `cash` |
| transactions | amount | Decimal greater than 0 and at most 100,000 |
| transactions | currency | `USD`, `EUR`, `GBP`, `JPY`, `CAD` |
| transactions | txn_status | `success`, `failed`, `pending`, `refund` |
| transactions | txn_ts | Valid timestamp and not in the future |

A row can fail several rules; all reasons are stored in `rejection_reason`, separated by `|`.

**Row counts by rule** (`TODO`: paste output of the rejection-reason queries in `sql/quality_assertions.sql`).

### 6.3 Processing precedence

For each dataset, every Bronze row is assigned to exactly one bucket:

1. Safe type conversion.
2. Validation against the rules matrix. **Failed rows go to quarantine.**
3. Among **valid rows only**, deduplicate on the business key (`order_id` / `txn_id`).
4. The kept row goes to `silver_*`. The non-winning rows go to `duplicate_*`.

Because deduplication runs only on rows that passed validation, no row can be in two buckets.

Orphan note: a transaction is checked against **accepted** `silver_orders`. A transaction whose order was quarantined is therefore also reported as `orphan_order_id`.

### 6.4 Deterministic deduplication

- **Keep-one rule:** `ROW_NUMBER()` over `PARTITION BY business_key ORDER BY _row_hash, _source_file, _batch_id`. The row with the smallest `_row_hash` wins; the rest are duplicate losers.
- **Duplicate key vs exact duplicate:** a *duplicate key* shares `order_id`/`txn_id` but may differ in other fields. An *exact duplicate* is identical in every source field and therefore has the same `_row_hash`. Exact duplicates are a special case of duplicate keys. Their rows are interchangeable, so the surviving data is the same whichever copy is kept.

![Example winners and losers](evidence/10_dedup_examples.png)

### 6.5 Reconciliation (the key proof)

`sql/quality_assertions.sql` checks, per dataset:

```text
Bronze rows = accepted Silver rows + quarantined rows + duplicate losers
```

| Dataset | Bronze | Accepted Silver | Quarantined | Duplicate losers | Sum | Equal? |
|---|---:|---:|---:|---:|---:|---|
| orders | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |
| transactions | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` | `TODO` |

![Reconciliation summaries](evidence/11_reconciliation.png)
![Quarantine sample with reasons](evidence/12_quarantine_sample.png)
![Duplicate losers sample](evidence/13_duplicate_losers_sample.png)

---

## 7. Gold: business-ready models

### 7.1 Revenue definition

**Source of truth:** `silver_transactions` joined once to `silver_orders` on `order_id` (many transactions to one unique order, so no values are multiplied).

> **Revenue = sum of `amount` for transactions with `txn_status = 'success'` and `currency = 'USD'`, whose order has `order_status = 'completed'`.**

- **Currency:** USD only. No exchange rates are invented; other currencies are excluded from revenue.
- **Cancellations and refunds:** excluded (order not `completed`, or transaction not `success`).
- **Partial and repeat payments:** each accepted successful `txn_id` counts as its own payment event.
- **Rounding:** none during aggregation; values are shown to two decimals.

| order_id | order status | txn_id | txn status | currency | amount | Revenue |
|---|---|---|---|---|---:|---:|
| O1 | completed | T1 | success | USD | 80.00 | 80.00 |
| O1 | completed | T2 | success | USD | 20.00 | 20.00 |
| O1 | cancelled | T3 | success | USD | 100.00 | 0.00 |
| O2 | completed | T4 | refund | USD | 15.00 | 0.00 |

Total comparable revenue = **100.00 USD**.

### 7.2 Gold tables, grain and keys

| Table | Grain | Unique key | Main columns |
|---|---|---|---|
| `gold_daily_sales` | One row per order date | `sales_date` | `total_revenue`, `order_count`, `cancelled_order_count`, `cancellation_rate` |
| `gold_revenue_breakdown` | One row per category and country | `category`, `country` | `total_revenue`, `successful_payment_count` |
| `gold_payment_success` | One row per payment method | `payment_method` | `payment_count`, `successful_count`, `failed_count`, `success_rate` |
| `gold_top_customers` | One row per customer | `customer_id` | `total_spend`, `successful_payment_count`, `order_count` |

**Metric assumptions:** `cancellation_rate = cancelled_order_count / order_count` (all accepted orders on that date). `success_rate = successful_count / payment_count` (all accepted transactions for that method). Revenue tables use the revenue definition in 7.1. `gold_revenue_breakdown` has no date column, so date filtering for revenue totals uses `gold_daily_sales`.

| Table | Row count | 
|---|---:|
| gold_daily_sales | `TODO` |
| gold_revenue_breakdown | `TODO` |
| gold_payment_success | `TODO` |
| gold_top_customers | `TODO` |

![Gold tables and sample output](evidence/14_gold_samples.png)

### 7.3 Documentation (comments)

Each Gold table and column has a `COMMENT` stating its meaning, grain and metric assumption (see `sql/gold_comments.sql`). `TODO`: add and run the comment statements, then screenshot Catalog Explorer.

**How documentation prevents misinterpretation:** a stated grain stops analysts from summing a rate or double counting; a stated revenue definition stops different reports from using different filters; recorded assumptions (USD only, completed orders only) make a number defensible when someone asks why it differs from another report.

![Catalog Explorer comments](evidence/15_gold_comments.png)

---

## 8. Databricks AI/BI

### 8.1 Dashboard

Built on the Gold tables, with:

- **4+ KPI counters:** total revenue, total orders, cancellation rate, payment success rate
- **1 time trend:** revenue by `sales_date`
- **2 breakdown visuals:** revenue by category, revenue by country
- **1 shared filter:** `TODO` (date range and/or country)

Link: `TODO`

![AI/BI dashboard](evidence/16_aibi_dashboard.png)

### 8.2 Genie Agent

Created over the four Gold tables. `TODO`: if Genie is unavailable in my trial, record that here and keep the three prompts and SQL below as the fallback.

| # | Prompt | Verification SQL |
|---|---|---|
| 1 | What is total successful USD revenue for completed orders? | `SELECT ROUND(SUM(total_revenue),2) FROM workspace.ecom_poc.gold_daily_sales;` |
| 2 | Which payment method has the highest success rate? | `SELECT payment_method, success_rate FROM workspace.ecom_poc.gold_payment_success ORDER BY success_rate DESC LIMIT 1;` |
| 3 | Who are the top five customers by spend? | `SELECT customer_id, total_spend FROM workspace.ecom_poc.gold_top_customers ORDER BY total_spend DESC LIMIT 5;` |

For each prompt, `TODO`: record Genie's answer, its generated SQL, my SQL result, and whether they match.

![Genie answers](evidence/17_genie_answers.png)

---

## 9. Power BI Desktop

### 9.1 Connection and mode

Connected with the native **Databricks connector** to the SQL warehouse (server hostname and HTTP path from the warehouse Connection details; secrets masked in screenshots). `TODO`: if blocked, use the fallback (Gold-only CSV exports) and say so in Section 12.

**Import vs DirectQuery:** Import copies the small Gold tables into the report. It is fast and does not need the warehouse running while people view the report. DirectQuery sends live queries to the warehouse for every interaction: data is always fresh, but it is slower and consumes warehouse credits. **Chosen mode: Import**, because Gold tables are small and the data only changes when the pipeline runs.

![Connection settings (secrets masked)](evidence/18_powerbi_connection.png)

### 9.2 Measures (same definitions as the dashboard)

```DAX
Total Revenue        = SUM(gold_daily_sales[total_revenue])
Total Orders         = SUM(gold_daily_sales[order_count])
Cancelled Orders     = SUM(gold_daily_sales[cancelled_order_count])
Cancellation Rate    = DIVIDE([Cancelled Orders], [Total Orders])
Category Revenue     = SUM(gold_revenue_breakdown[total_revenue])
Payment Success Rate = DIVIDE(SUM(gold_payment_success[successful_count]),
                              SUM(gold_payment_success[payment_count]))
```

Rates are computed from summed counts, so they stay correct under slicers.

One-page report: KPI cards (revenue, orders, cancellation rate, payment success rate), daily revenue trend, revenue by category, and slicers (`TODO`: date, category, country). File: `powerbi/<name>.pbix`.

![Power BI report](evidence/19_powerbi_report.png)

### 9.3 Cross-tool revenue verification

Same date range, currency (USD), order status (`completed`) and transaction status (`success`). SQL in `sql/cross_tool_validation.sql`.

| Source | Total revenue | Date range |
|---|---:|---|
| Databricks SQL | `TODO` | `TODO` |
| AI/BI dashboard | `TODO` | `TODO` |
| Power BI report | `TODO` | `TODO` |

Match? `TODO`. If any value differs, diagnose here (filter mismatch, date range, currency scope, rounding or stale import).

![Three-way comparison](evidence/20_revenue_comparison.png)

---

## 10. Lakeflow Job

Tasks: `bronze` → `silver` → `gold`, with dependencies, retries and failure notification.

| Setting | Value |
|---|---|
| Compute | Serverless (`TODO` confirm) |
| Dependencies | silver depends on bronze; gold depends on silver |
| Retries | `TODO` (per task) |
| Failure notification | `TODO` (email) |
| Schedule | None (manual run) |
| Result | `TODO` successful run, duration |

**Why Gold does not run after a failed dependency:** by default a task runs only if everything it depends on succeeded. If Silver fails, Gold would be built from missing or stale data and publish wrong numbers that look correct. Stopping the chain is the safe behaviour.

![Job graph](evidence/21_job_graph.png)
![Successful run](evidence/22_job_run.png)
![Retry and notification settings](evidence/23_job_settings.png)

---

## 11. Security, least privilege and cleanup

### Least-privilege reporting access

A reporting group gets only `USE CATALOG`, `USE SCHEMA` and `SELECT` on the four Gold tables, and nothing on Bronze, Silver, quarantine, duplicate tables or the raw volume. Statements are in `sql/grants.sql`. `TODO`: if I cannot add a reporting user in the trial, record the restriction and show `SHOW GRANTS` output.

How an administrator would test: the reporting identity can `SELECT` from Gold; is denied on `bronze_orders` and `silver_transactions`; cannot write or manage tables; cannot read the raw volume.

![Permissions evidence](evidence/24_permissions.png)

### End-of-trial checklist

- [ ] Stop the SQL warehouse
- [ ] Disable any job schedule
- [ ] Stop notebook compute
- [ ] Check and record remaining credits: `TODO`
- [ ] Export notebooks, dashboards, Power BI file and screenshots
- [ ] Commit and push all changes
- [ ] Confirm no tokens, credentials or PII in Git or screenshots

---

## 12. Trial limitations and fallbacks

`TODO`: list every limitation actually encountered and the fallback used. Write "none" where nothing was restricted.

| Area | Limitation observed | Fallback used |
|---|---|---|
| Account / workspace | `TODO` | `TODO` |
| Unity Catalog / users | `TODO` | `TODO` |
| Genie Agent | `TODO` | `TODO` |
| Dashboard sharing | `TODO` | `TODO` |
| Power BI connector | `TODO` | `TODO` |
| Git integration | `TODO` | `TODO` |

## 13. Reproduce this project

1. Create a Databricks free trial with your own email; create the schema and volume (`sql/setup.sql`).
2. Run `python generator/generate_synthetic_data.py` and upload both CSVs to `/Volumes/workspace/ecom_poc/raw_files/`.
3. Run `notebooks/` in order: setup, Bronze, Silver, Gold (or run the Lakeflow Job).
4. Run `sql/quality_assertions.sql`, `sql/gold_metrics.sql` and `sql/cross_tool_validation.sql`.
5. Build the AI/BI dashboard and Power BI report from the Gold tables.

## 14. Evidence checklist

- [ ] Own-email Databricks trial workspace (01, 02)
- [ ] Catalog, schema, volume setup (03)
- [ ] Compute and warehouse (04)
- [ ] Git commit history (05)
- [ ] Source row counts and byte sizes (06)
- [ ] Bronze schema and reconciliation (07, 08)
- [ ] Silver conversion, deduplication, quarantine, reconciliation (09 to 13)
- [ ] Gold outputs and comments (14, 15)
- [ ] AI/BI dashboard and Genie checks (16, 17)
- [ ] Power BI report and revenue comparison (18 to 20)
- [ ] Lakeflow successful run (21 to 23)
- [ ] Least-privilege grants and cleanup (24)
