
# Retail Data Engineering Project

End-to-end AWS Data Pipeline using S3, Glue, Athena.

## Flow
Postgres/MySQL → Python → S3 → Glue → Processed → Gold → Athena

## Run
1. Update DB credentials
2. python scripts/run_pipeline.py
3. Run Glue Jobs
4. Query in Athena
