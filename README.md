# Fitness Data Pipeline

Modern data stack project that ingests fitness and nutrition data from multiple sources and models it in Snowflake using dbt to analyze training load, nutrition intake, and body metrics.

Overview

This project is a personal fitness analytics platform that integrates activity data from the Strava API and nutrition/measurement data from MyFitnessPal exports.

The pipeline ingests, stores, and transforms the data using a modern data stack built with:

- Python
- Docker
- Kubernetes
- AWS S3
- Snowflake
- dbt

The goal is to analyze relationships between training activity, nutrition intake, and body measurements using a reproducible data pipeline and warehouse modeling workflow.

Architecture:

Strava ingestion pipeline:
Strava API
      │
      ▼
Python ingestion scripts
      │
      ▼
Docker container
      │
      ▼
Kubernetes CronJob (scheduled ingestion)
      │
      ▼
AWS S3 (raw storage)
      │
      ▼
Snowflake (raw tables)
      │
      ▼
dbt transformations
      │
      ▼
Analytics models

MyFitnessPal data follows a batch ingestion workflow:
MyFitnessPal CSV Exports
      │
      ▼
Python ingestion scripts
      │
      ▼
AWS S3
      │
      ▼
Snowflake raw tables

In production, the custom ingestion layer could be replaced with a managed connector like Fivetran.

Tech Stack:
| Layer            | Tools                                |
| ---------------- | ------------------------------------ |
| Source systems   | Strava API, MyFitnessPal CSV exports |
| Ingestion        | Python                               |
| Containerization | Docker                               |
| Orchestration    | Kubernetes CronJobs                  |
| Storage          | AWS S3                               |
| Data warehouse   | Snowflake                            |
| Transformation   | dbt                                  |
| Version control  | Git + GitHub                         |

dbt Models:
The warehouse layer transforms raw activity and nutrition data into analytics-ready tables.

activity_summary
Aggregates Strava activity data to analyze:
- activity type
- duration
- distance
- training volume

weekly_activity_volume
Calculates weekly training metrics including:
- total distance
- total moving time
- activity counts

daily_training_vs_nutrition
Joins training activity with nutrition intake to analyze:
- calories consumed vs training load
- training duration vs caloric intake
- activity days vs rest days

Project Structure:
fitness-data-pipeline
│
├── ingestion
│   ├── strava_to_s3.py
│   ├── mfp_nutrition_to_s3.py
│   ├── mfp_exercise_to_s3.py
│   └── mfp_measurements_to_s3.py
│
├── data
│   └── raw exports
│
├── dbt_project
│   ├── models
│   │   ├── activity_summary.sql
│   │   ├── weekly_activity_volume.sql
│   │   └── daily_training_vs_nutrition.sql
│
├── Dockerfile
├── requirements.txt
└── README.md

Running the Pipeline:
1. Run ingestion locally
python ingestion/strava_to_s3.py
2. Run dbt models
cd dbt_project
dbt run

Production Architecture:
In a production environment, the custom ingestion layer could be replaced with a managed connector like Fivetran.

Source Systems
(Strava, MyFitnessPal)
      │
      ▼
Fivetran
      │
      ▼
Snowflake
      │
      ▼
dbt models
      │
      ▼
Analytics tables

This approach would provide:

managed data extraction

automatic schema evolution

reliable incremental loading

reduced operational overhead