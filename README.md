# Financial Data Pipeline — AWS Glue, S3, Snowflake, Airflow

End-to-end data pipeline that ingests, transforms, and loads
large-scale financial data using AWS native services.

## Tech Stack
- Python, PySpark, AWS Glue, AWS S3
- Apache Airflow (MWAA) — orchestration
- Snowflake — data warehouse
- Parquet / Delta Lake — storage formats
- Terraform — infrastructure as code

## Pipeline Flow
1. Raw financial data lands in S3 (Bronze layer)
2. AWS Glue job cleans and transforms data (Silver layer)
3. Aggregated business metrics loaded into Snowflake (Gold layer)
4. Airflow DAG schedules runs, handles retries and failure alerts
5. CloudWatch monitors pipeline health and sends alerts

## Key Highlights
- Designed for high-volume data processing (5+ TB daily)
- Medallion Architecture (Bronze/Silver/Gold)
- CI/CD deployment with Terraform and GitHub Actions
