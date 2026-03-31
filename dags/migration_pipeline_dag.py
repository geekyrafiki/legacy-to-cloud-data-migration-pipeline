from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.main import run_pipeline


default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="legacy_to_cloud_data_migration_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Migration-focused ETL pipeline using PostgreSQL, Python, and Airflow.",
    tags=["data-engineering", "migration", "postgresql"],
) as dag:
    run_migration = PythonOperator(
        task_id="run_migration_pipeline",
        python_callable=run_pipeline,
    )

    run_migration
