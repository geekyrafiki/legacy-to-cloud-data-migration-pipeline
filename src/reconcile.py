from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logger import get_logger

LOGGER = get_logger()

SOURCE_TO_TARGET_MAP = {
    "patients": "warehouse.dim_patient",
    "appointments": "warehouse.fact_appointment",
    "billing_transactions": "warehouse.fact_billing",
}


def run_reconciliation(source_data: dict) -> list[str]:
    LOGGER.info("Running reconciliation checks.")
    results: list[str] = []

    engine = get_engine()

    with engine.connect() as connection:
        for source_name, target_table in SOURCE_TO_TARGET_MAP.items():
            source_count = len(source_data[source_name])
            target_count = connection.execute(
                text(f"SELECT COUNT(*) FROM {target_table}")
            ).scalar()

            if source_count == target_count:
                results.append(
                    f"PASS: Reconciliation matched for {source_name} -> {target_table} "
                    f"({source_count} rows)."
                )
            else:
                results.append(
                    f"WARN: Reconciliation mismatch for {source_name} -> {target_table}. "
                    f"Source={source_count}, Target={target_count}"
                )

    LOGGER.info("Reconciliation checks completed.")
    return results