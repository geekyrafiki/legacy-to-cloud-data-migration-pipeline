# Legacy-to-Cloud Data Migration Pipeline

This repository showcases an end-to-end data migration pipeline built with PostgreSQL, SQL scripts, Python, and Apache Airflow. It is designed to simulate a real-world legacy-to-cloud style migration workflow, where data is extracted from legacy source tables, transformed and standardized through Python-based business logic, validated for data quality and referential integrity, loaded into a modern target schema, and reconciled to confirm migration accuracy.

The project focuses on the types of engineering challenges commonly found in migration work, including source-to-target mapping, legacy schema cleanup, duplicate handling, invalid references, validation checks, and post-load reconciliation. It is structured to reflect production-style data engineering practices by separating SQL artifacts, transformation logic, validation logic, and orchestration into clear, maintainable components.

The purpose of this project is to demonstrate practical data engineering skills that reflects business-focused migration and pipeline design.

---

## Key Features

- PostgreSQL-based source and target environments
- SQL scripts for schema creation, validation, and reconciliation
- Python-driven extraction, transformation, and loading logic
- Apache Airflow DAG for workflow orchestration
- Data quality validation for nulls, duplicates, and invalid references
- Source-to-target reconciliation checks after load
- Modular project structure designed for maintainability and readability

---

## Why I Built This Project

I built this project to reflect the type of work involved in real migration and data engineering roles. My background includes working with complex legacy data, transformation logic, data quality validation, and migration workflows, so this repository was designed to mirror those same challenges in a structured portfolio project.

Instead of creating a generic ETL demo, I wanted this project to show how a migration pipeline should be organized when data integrity, traceability, and validation matter. The result is a project that demonstrates not just coding ability, but also engineering judgment around data movement and operational reliability.

---

## Project Overview

This pipeline simulates the migration of legacy operational data into a cleaner target schema. The legacy environment contains raw source tables with inconsistent formats and values. The pipeline extracts that data, applies business and formatting rules, validates the transformed outputs, loads them into target warehouse-style tables, and then runs reconciliation checks to compare source and target record counts.

The workflow is designed to demonstrate a realistic migration pattern:

1. Extract data from legacy PostgreSQL source tables  
2. Apply transformation and cleansing logic with Python  
3. Validate transformed data for quality and integrity  
4. Load cleaned data into PostgreSQL target tables  
5. Reconcile source and target datasets  
6. Orchestrate execution through Apache Airflow  

---

## Architecture

The project is divided into four main layers:

**Source Layer**  
Legacy-style PostgreSQL tables store raw operational data such as patients, appointments, and billing transactions.

**Transformation Layer**  
Python modules clean and standardize the source data by applying formatting rules, duplicate handling, type conversion, and filtering of invalid records.

**Target Layer**  
A cleaner PostgreSQL target schema stores transformed dimension and fact-style tables for downstream use.

**Orchestration Layer**  
Apache Airflow manages execution of the pipeline through a DAG that runs the migration process in a controlled and repeatable way.

---

## Tech Stack

**Languages and Libraries**  
Python, SQL, Pandas, PyYAML, SQLAlchemy, psycopg2

**Database**  
PostgreSQL

**Orchestration**  
Apache Airflow

**Testing**  
Pytest

**Environment**  
Docker Compose for local PostgreSQL setup

---

## Repository Structure

```text
legacy-to-cloud-data-migration-pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
├── docker-compose.yml
│
├── config/
│   ├── source_config.yaml
│   ├── target_config.yaml
│   └── field_mappings.yaml
│
├── sql/
│   ├── source_schema.sql
│   ├── target_schema.sql
│   ├── validation_queries.sql
│   └── reconciliation_queries.sql
│
├── dags/
│   └── migration_pipeline_dag.py
│
├── src/
│   ├── main.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── validate.py
│   ├── reconcile.py
│   └── utils/
│       ├── db.py
│       ├── logger.py
│       └── config_loader.py
│
└── tests/
    ├── test_transform_patients.py
    └── test_validation_rules.py