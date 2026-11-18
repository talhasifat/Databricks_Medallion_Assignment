USE CATALOG workspace;
USE SCHEMA ecom_poc;

-- Comparable USD revenue definition:
-- successful transaction + completed order + USD + transaction grain
SELECT round(sum(amount), 2) AS total_revenue_usd
FROM silver_transactions t
JOIN silver_orders o USING (order_id)
WHERE t.txn_status = 'success'
  AND t.currency = 'USD'
  AND o.order_status = 'completed';

SELECT * FROM gold_daily_sales ORDER BY sales_date;
SELECT * FROM gold_revenue_breakdown ORDER BY total_revenue DESC;
SELECT * FROM gold_payment_success ORDER BY success_rate DESC;
SELECT * FROM gold_top_customers LIMIT 20;

-- Stable same-filter verification example.
-- Replace dates and apply the same currency/status filter in AI/BI and Power BI.
SELECT
  round(sum(t.amount), 2) AS total_revenue_usd
FROM silver_transactions t
JOIN silver_orders o USING (order_id)
WHERE o.order_date BETWEEN '2025-01-01' AND '2026-12-31'
  AND t.txn_status = 'success'
  AND t.currency = 'USD'
  AND o.order_status = 'completed';
