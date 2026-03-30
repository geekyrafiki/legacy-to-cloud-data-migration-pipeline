# Legacy-to-Cloud Data Migration Pipeline

This repository showcases an end-to-end data migration pipeline built with PostgreSQL, SQL scripts, Python, and Apache Airflow. It simulates a legacy-to-cloud style migration workflow where legacy operational data is extracted from PostgreSQL source tables, transformed and standardized with Python, validated for data quality and referential integrity, loaded into a cleaner target schema, and reconciled to confirm migration accuracy.

The project is intentionally designed to look and behave like a migration-oriented data engineering repository rather than a tutorial-only ETL demo. It emphasizes source-to-target mapping, legacy schema cleanup, duplicate handling, rejected-record management, post-load validation, reconciliation reporting, and a clean modular structure.

## Key Features

- PostgreSQL source and target schemas in the same database for local development
- SQL artifacts for schema creation, validation, and reconciliation
- Config-driven field mappings through YAML
- Python-driven extraction, transformation, loading, validation, and reporting
- Rejected-record outputs for invalid downstream rows
- Text and CSV validation/reconciliation reports
- Apache Airflow DAG included for orchestration showcase
- Pytest coverage for core transformation and validation behavior

## Repository Structure

```text
legacy-to-cloud-data-migration-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
├── Makefile
├── docker-compose.yml
├── config/
│   ├── source_config.yaml
│   ├── target_config.yaml
│   └── field_mappings.yaml
├── data/
│   ├── raw/
│   │   ├── patients.csv
│   │   ├── appointments.csv
│   │   └── billing_transactions.csv
│   ├── rejected/
│   └── reports/
├── dags/
│   └── migration_pipeline_dag.py
├── sql/
│   ├── source_schema.sql
│   ├── target_schema.sql
│   ├── validation_queries.sql
│   └── reconciliation_queries.sql
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── seed_legacy.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── validate.py
│   ├── reconcile.py
│   ├── report.py
│   └── utils/
│       ├── __init__.py
│       ├── config_loader.py
│       ├── db.py
│       └── logger.py
└── tests/
    ├── test_transform.py
    └── test_validate.py
```

## Data Model

### Source Schema: `legacy`
The source layer simulates a legacy operational system.

- `legacy.legacy_patients`
- `legacy.legacy_appointments`
- `legacy.legacy_billing_transactions`

### Target Schema: `warehouse`
The target layer holds cleaned data that is easier to trust and consume.

- `warehouse.dim_patient`
- `warehouse.fact_appointment`
- `warehouse.fact_billing`

## What the Pipeline Does

1. Creates the legacy and warehouse schemas from SQL scripts.
2. Loads sample legacy CSV files into the `legacy` schema.
3. Extracts source data into Pandas DataFrames.
4. Applies field mappings and business rules.
5. Rejects invalid child records instead of letting the load fail.
6. Loads clean tables into the `warehouse` schema.
7. Runs validation checks and reconciliation checks.
8. Writes reports to `data/reports/` and logs to `logs/`.

## Transformation and Quality Logic

Examples of logic included in this project:

- Renames legacy columns to target-friendly names using YAML mappings
- Standardizes names, emails, status values, and code fields
- Converts mixed date strings into date/timestamp types
- Drops duplicate primary keys
- Rejects appointment and billing rows that reference missing patients
- Validates row counts, null keys, duplicate keys, and invalid foreign keys
- Reconciles source counts against loaded target counts

## How to Run

### 1. Start PostgreSQL

```bash
docker compose up -d postgres
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Seed the legacy schema

```bash
python -m src.seed_legacy
```

### 4. Run the migration pipeline

```bash
python -m src.main
```

### 5. Run tests

```bash
pytest -q
```

## Airflow

The DAG in `dags/migration_pipeline_dag.py` is included to show how the pipeline can be orchestrated in Apache Airflow. It wraps the same migration entry point used by the standalone pipeline.

## What This Project Demonstrates

This repository is meant to show employers that I can:

- design a migration-style ETL pipeline
- separate SQL artifacts from Python pipeline logic
- work with PostgreSQL as a source and target environment
- build transformation rules that account for messy legacy data
- validate and reconcile results rather than just move records
- structure a portfolio repository like maintainable engineering work

## Future Improvements

- Add data quality thresholds and pipeline fail-fast rules
- Add incremental batch support and audit columns
- Add more unit tests and CI
- Add a fully containerized local Airflow environment
- Extend reporting to HTML or dashboard outputs

## Notes

This project uses synthetic data only. It is designed for portfolio and interview demonstration purposes.
