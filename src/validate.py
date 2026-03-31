import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logger import get_logger

LOGGER = get_logger()


def run_validation_checks(
    transformed_data: dict[str, pd.DataFrame]
) -> list[str]:
    LOGGER.info("Running validation checks.")
    results: list[str] = []

    dim_patient = transformed_data["dim_patient"]
    fact_appointment = transformed_data["fact_appointment"]
    fact_billing = transformed_data["fact_billing"]

    if dim_patient["patient_id"].isnull().any():
        results.append("FAIL: Null patient_id values found in dim_patient.")
    else:
        results.append("PASS: No null patient_id values found in dim_patient.")

    if dim_patient["patient_id"].duplicated().any():
        results.append("FAIL: Duplicate patient_id values found in dim_patient.")
    else:
        results.append("PASS: No duplicate patient_id values found in dim_patient.")

    valid_patient_ids = set(dim_patient["patient_id"].tolist())

    invalid_appointments = fact_appointment[
        ~fact_appointment["patient_id"].isin(valid_patient_ids)
    ]
    if not invalid_appointments.empty:
        results.append("FAIL: Invalid patient references found in fact_appointment.")
    else:
        results.append("PASS: All patient references in fact_appointment are valid.")

    invalid_billing = fact_billing[
        ~fact_billing["patient_id"].isin(valid_patient_ids)
    ]
    if not invalid_billing.empty:
        results.append("FAIL: Invalid patient references found in fact_billing.")
    else:
        results.append("PASS: All patient references in fact_billing are valid.")

    engine = get_engine()

    with engine.connect() as connection:
        row_count_queries = {
            "dim_patient": "SELECT COUNT(*) FROM warehouse.dim_patient",
            "fact_appointment": "SELECT COUNT(*) FROM warehouse.fact_appointment",
            "fact_billing": "SELECT COUNT(*) FROM warehouse.fact_billing",
        }

        for table_name, query in row_count_queries.items():
            db_count = connection.execute(text(query)).scalar()
            df_count = len(transformed_data[table_name])

            if db_count == df_count:
                results.append(f"PASS: Row count matches for {table_name} ({df_count} rows).")
            else:
                results.append(
                    f"FAIL: Row count mismatch for {table_name}. "
                    f"DataFrame={df_count}, DB={db_count}"
                )

    LOGGER.info("Validation checks completed.")
    return results