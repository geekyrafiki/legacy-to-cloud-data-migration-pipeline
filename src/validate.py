import pandas as pd
from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logger import get_logger

LOGGER = get_logger()


def build_in_memory_validation_results(
    clean_tables: dict[str, pd.DataFrame],
    rejected_tables: dict[str, pd.DataFrame] | None = None
) -> list[dict]:
    rejected_tables = rejected_tables or {}
    results: list[dict] = []

    dim_patient = clean_tables["dim_patient"]
    fact_appointment = clean_tables["fact_appointment"]
    fact_billing = clean_tables["fact_billing"]

    results.append({
        "check_name": "dim_patient_null_patient_id",
        "status": "PASS" if not dim_patient["patient_id"].isnull().any() else "FAIL",
        "details": (
            "No null patient_id values found in dim_patient."
            if not dim_patient["patient_id"].isnull().any()
            else "Null patient_id values found in dim_patient."
        ),
    })

    results.append({
        "check_name": "dim_patient_duplicate_patient_id",
        "status": "PASS" if not dim_patient["patient_id"].duplicated().any() else "FAIL",
        "details": (
            "No duplicate patient_id values found in dim_patient."
            if not dim_patient["patient_id"].duplicated().any()
            else "Duplicate patient_id values found in dim_patient."
        ),
    })

    valid_patient_ids = set(dim_patient["patient_id"].tolist())

    invalid_appointments = fact_appointment[
        ~fact_appointment["patient_id"].isin(valid_patient_ids)
    ]
    results.append({
        "check_name": "fact_appointment_invalid_patient_refs",
        "status": "PASS" if invalid_appointments.empty else "FAIL",
        "details": (
            "No invalid patient references found in fact_appointment."
            if invalid_appointments.empty
            else "Invalid patient references found in fact_appointment."
        ),
    })

    invalid_billing = fact_billing[
        ~fact_billing["patient_id"].isin(valid_patient_ids)
    ]
    results.append({
        "check_name": "fact_billing_invalid_patient_refs",
        "status": "PASS" if invalid_billing.empty else "FAIL",
        "details": (
            "No invalid patient references found in fact_billing."
            if invalid_billing.empty
            else "Invalid patient references found in fact_billing."
        ),
    })

    return results


def run_validation_checks(transformed_data: dict[str, pd.DataFrame]) -> list[dict]:
    LOGGER.info("Running validation checks.")

    if "clean_tables" in transformed_data:
        clean_tables = transformed_data["clean_tables"]
        rejected_tables = transformed_data.get("rejected_tables", {})
    else:
        clean_tables = {
            "dim_patient": transformed_data["dim_patient"],
            "fact_appointment": transformed_data["fact_appointment"],
            "fact_billing": transformed_data["fact_billing"],
        }
        rejected_tables = {}

    results = build_in_memory_validation_results(clean_tables, rejected_tables)

    engine = get_engine()

    row_count_queries = {
        "dim_patient": "SELECT COUNT(*) FROM warehouse.dim_patient",
        "fact_appointment": "SELECT COUNT(*) FROM warehouse.fact_appointment",
        "fact_billing": "SELECT COUNT(*) FROM warehouse.fact_billing",
    }

    with engine.connect() as connection:
        for table_name, query in row_count_queries.items():
            db_count = connection.execute(text(query)).scalar()
            df_count = len(clean_tables[table_name])

            results.append({
                "check_name": f"{table_name}_row_count_match",
                "status": "PASS" if db_count == df_count else "FAIL",
                "details": (
                    f"Row count matches for {table_name} ({df_count} rows)."
                    if db_count == df_count
                    else f"Row count mismatch for {table_name}. DataFrame={df_count}, DB={db_count}"
                ),
            })

    LOGGER.info("Validation checks completed.")
    return results