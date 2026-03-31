import os
from src.extract import extract_postgres_data
from src.transform import transform_all_data
from src.load import create_target_tables, truncate_target_tables, load_dataframes_to_postgres
from src.validate import run_validation_checks
from src.reconcile import run_reconciliation
from src.utils.logger import get_logger

LOGGER = get_logger()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_SCHEMA_SQL = os.path.join(BASE_DIR, "sql", "target_schema.sql")


def run_pipeline() -> None:
    LOGGER.info("Starting migration pipeline.")

    source_data = extract_postgres_data()
    transformed_data = transform_all_data(source_data)

    create_target_tables(TARGET_SCHEMA_SQL)
    truncate_target_tables()
    load_dataframes_to_postgres(transformed_data)

    validation_results = run_validation_checks(transformed_data)
    reconciliation_results = run_reconciliation(source_data)

    LOGGER.info("Validation Results:")
    for result in validation_results:
        LOGGER.info(result)

    LOGGER.info("Reconciliation Results:")
    for result in reconciliation_results:
        LOGGER.info(result)

    LOGGER.info("Migration pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()