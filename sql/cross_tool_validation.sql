USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Record this exact revenue and date filter in Databricks AI/BI and Power BI.
SELECT
  'cross_tool' AS check_name,
  CAST(MIN(o.order_date) AS STRING) AS from_date,
  CAST(MAX(o.order_date) AS STRING) AS to_date,
  'USD' AS currency,
  'success' AS txn_status,
  'completed' AS order_status,
  round(sum(t.amount), 2) AS total_revenue
FROM silver_transactions t
JOIN silver_orders o USING (order_id)
WHERE t.txn_status = 'success'
  AND t.currency = 'USD'
  AND o.order_status = 'completed';
