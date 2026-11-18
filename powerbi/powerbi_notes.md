# Power BI evidence

Power BI Desktop is not produced locally because no Databricks trial credentials or `.pbix` authoring environment are available in this workspace.

For actual submission:

1. Use the Databricks native SQL connector with Import mode.
2. Mask server, warehouse, and authentication details in screenshots.
3. Import all four Gold tables.
4. Prefer Import for this synthetic workload unless DirectQuery is specifically required.
5. Create at least:
   - total revenue;
   - cancellation rate;
   - payment success rate;
   - total successful customers or total orders.
6. Add a one-page report with KPI cards, a daily trend, a category chart, and slicers.
7. Record `cross_tool_validation.sql` revenue with the same date range and filters.

If native connector access is blocked, export only the four Gold table outputs as small CSVs and disclose the fallback.

