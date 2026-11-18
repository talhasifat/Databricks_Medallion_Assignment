-- Intended least-privilege reporting grants.
-- Run by a Unity Catalog account/catalog admin.
USE CATALOG workspace;
USE SCHEMA ecom_poc;

GRANT USE CATALOG ON CATALOG workspace TO `reporting_users`;
GRANT USE SCHEMA ON SCHEMA workspace.ecom_poc TO `reporting_users`;

GRANT SELECT ON TABLE workspace.ecom_poc.gold_daily_sales TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_revenue_breakdown TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_payment_success TO `reporting_users`;
GRANT SELECT ON TABLE workspace.ecom_poc.gold_top_customers TO `reporting_users`;

-- Validation:
-- SHOW GRANTS ON TABLE workspace.ecom_poc.gold_daily_sales;
-- SHOW GRANTS ON SCHEMA workspace.ecom_poc;
-- Test with a reporting identity: SELECT * FROM gold_daily_sales LIMIT 10;
-- The same identity must be denied SELECT on bronze_orders and silver_transactions.
