-- Replace workspace/ecom_poc when running in a different trial catalog/schema.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

SELECT
  'orders' AS dataset,
  (SELECT count(*) FROM bronze_orders) AS bronze_rows,
  (SELECT count(*) FROM silver_orders) AS accepted_rows,
  (SELECT count(*) FROM quarantine_orders) AS quarantine_rows,
  (SELECT count(*) FROM duplicate_orders) AS duplicate_loser_rows,
  (SELECT count(*) FROM bronze_orders)
    = (SELECT count(*) FROM silver_orders)
    + (SELECT count(*) FROM quarantine_orders)
    + (SELECT count(*) FROM duplicate_orders) AS exact_reconciliation;

SELECT
  'transactions' AS dataset,
  (SELECT count(*) FROM bronze_transactions) AS bronze_rows,
  (SELECT count(*) FROM silver_transactions) AS accepted_rows,
  (SELECT count(*) FROM quarantine_transactions) AS quarantine_rows,
  (SELECT count(*) FROM duplicate_transactions) AS duplicate_loser_rows,
  (SELECT count(*) FROM bronze_transactions)
    = (SELECT count(*) FROM silver_transactions)
    + (SELECT count(*) FROM quarantine_transactions)
    + (SELECT count(*) FROM duplicate_transactions) AS exact_reconciliation;

SELECT rejection_reason, count(*) AS row_count
FROM quarantine_orders
GROUP BY rejection_reason
ORDER BY row_count DESC;

SELECT rejection_reason, count(*) AS row_count
FROM quarantine_transactions
GROUP BY rejection_reason
ORDER BY row_count DESC;

SELECT order_id, count(*) AS duplicate_loser_count
FROM duplicate_orders
GROUP BY order_id
ORDER BY duplicate_loser_count DESC
LIMIT 20;

SELECT txn_id, count(*) AS duplicate_loser_count
FROM duplicate_transactions
GROUP BY txn_id
ORDER BY duplicate_loser_count DESC
LIMIT 20;
