# Fitness Data Pipeline

Pipeline that ingests Strava fitness data and loads it into Snowflake.

Architecture:

Strava API → Python → S3 → Snowflake → dbt → dashboard