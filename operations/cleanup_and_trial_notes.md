# Trial and cleanup notes

Record candidate-specific account facts here after creating the own-email Databricks trial:

- Workspace URL:
- Cloud/provider:
- Region:
- Trial start:
- Trial expiry:
- Remaining credits:
- Catalog:
- SQL warehouse name and size:
- Notebook compute choice:
- Account restrictions:
- Git provider and repository URL:

## Compute strategy

- Use serverless or an all-purpose notebook only for ETL development.
- Use one small SQL warehouse for AI/BI and Power BI report validation.
- Stop the SQL warehouse after report work.
- Leave no schedule enabled unless it is a deliberately short test.
- Use one successful unscheduled Lakeflow run for evidence.

## Cleanup checklist

- [ ] Disable job schedule.
- [ ] Stop SQL warehouse.
- [ ] Stop or terminate notebook/cluster compute.
- [ ] Export notebooks, reports, SQL, and screenshots.
- [ ] Commit and push all repository changes.
- [ ] Record remaining credits.
- [ ] Remove sensitive connection settings and masks from screenshots.
- [ ] Remove or isolate source CSVs if local retention is no longer permitted.
- [ ] Confirm no credentials or tokens are in Git.

