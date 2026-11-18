# Least-privilege reporting access

The reporting identity should receive `USE CATALOG`, `USE SCHEMA`, and `SELECT` on only:

- `gold_daily_sales`
- `gold_revenue_breakdown`
- `gold_payment_success`
- `gold_top_customers`

Bronze and Silver tables and the raw volume must not be granted to the reporting identity.

Executable grants are in `sql/grants.sql`. If the candidate cannot add a reporting user or group in the trial:

1. Record the account restriction.
2. Show `SHOW GRANTS` for the candidate identity.
3. Keep the planned group principal and SQL grants in source control.
4. Explain how an administrator would test:
   - reporting identity can query Gold;
   - reporting identity cannot read Bronze/Silver/quarantine tables;
   - reporting identity cannot write or manage tables;
   - reporting identity cannot use the raw volume.

