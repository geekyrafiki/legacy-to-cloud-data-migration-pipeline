from __future__ import annotations

from pathlib import Path

from src.extract import extract_postgres_data
from src.load import create_target_tables, truncate_target_tables, TARGET_CONFIG_PATH
from src.reconcile import run_reconciliation
from src.report import write_reconciliation_report, write_rejected_outputs, write_validation_report
from src.transform import transform_all_data
from src.validate import run_validation_checks
from src.utils.logger import get_logger


LOGGER = get_logger()
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_pipeline() -> None:
    LOGGER.info("Starting migration pipeline.")

    LOGGER.info("Extracting source data from legacy schema.")
    source_data = extract_postgres_data()

    LOGGER.info("Transforming source data.")
    transformation_output = transform_all_data(source_data)
    clean_tables = transformation_output["clean_tables"]
    rejected_tables = transformation_output["rejected_tables"]

    LOGGER.info("Creating target schema and tables.")
    create_target_tables(TARGET_CONFIG_PATH)

    LOGGER.info("Truncating existing target data.")
    truncate_target_tables(TARGET_CONFIG_PATH)

    LOGGER.info("Loading clean tables into warehouse schema.")
    from src.load import load_dataframes_to_postgres
    load_dataframes_to_postgres(clean_tables, TARGET_CONFIG_PATH)

    LOGGER.info("Running validation checks.")
    validation_results = run_validation_checks(clean_tables, rejected_tables)

    LOGGER.info("Running reconciliation checks.")
    reconciliation_results = run_reconciliation(source_data, clean_tables)

    LOGGER.info("Writing output reports.")
    validation_report_path = write_validation_report(validation_results)
    reconciliation_report_path = write_reconciliation_report(reconciliation_results)
    rejected_paths = write_rejected_outputs(rejected_tables)

    LOGGER.info("Validation report written to %s", validation_report_path)
    LOGGER.info("Reconciliation report written to %s", reconciliation_report_path)
    if rejected_paths:
        LOGGER.info("Rejected record files written: %s", ", ".join(str(path) for path in rejected_paths))
    else:
        LOGGER.info("No rejected record files were generated.")

    LOGGER.info("Migration pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
