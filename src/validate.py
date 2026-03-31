from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET_CONFIG_PATH = PROJECT_ROOT / "config" / "target_config.yaml"


def build_in_memory_validation_results(
    clean_tables: dict[str, pd.DataFrame],
    rejected_tables: dict[str, pd.DataFrame],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    dim_patient = clean_tables["dim_patient"]
    fact_appointment = clean_tables["fact_appointment"]
    fact_billing = clean_tables["fact_billing"]

    def add_result(check_name: str, status: str, details: str) -> None:
        results.append({"check_name": check_name, "status": status, "details": details})

    has_null_patient_id = dim_patient["patient_id"].isna().any()
    add_result(
        "dim_patient_null_patient_id",
        "PASS" if not has_null_patient_id else "FAIL",
        "No null patient_id values found in dim_patient."
        if not has_null_patient_id
        else "Null patient_id values found in dim_patient.",
    )

    has_duplicate_patient_id = dim_patient["patient_id"].duplicated().any()
    add_result(
        "dim_patient_duplicate_patient_id",
        "PASS" if not has_duplicate_patient_id else "FAIL",
        "No duplicate patient_id values found in dim_patient."
        if not has_duplicate_patient_id
        else "Duplicate patient_id values found in dim_patient.",
    )

    valid_patient_ids = set(dim_patient["patient_id"].tolist())

    invalid_appointments = fact_appointment[~fact_appointment["patient_id"].isin(valid_patient_ids)]
    add_result(
        "fact_appointment_invalid_patient_refs",
        "PASS" if invalid_appointments.empty else "FAIL",
        "All patient_id values in fact_appointment map to dim_patient."
        if invalid_appointments.empty
        else f"Found {len(invalid_appointments)} invalid patient_id references in fact_appointment.",
    )

    invalid_billing = fact_billing[~fact_billing["patient_id"].isin(valid_patient_ids)]
    add_result(
        "fact_billing_invalid_patient_refs",
        "PASS" if invalid_billing.empty else "FAIL",
        "All patient_id values in fact_billing map to dim_patient."
        if invalid_billing.empty
        else f"Found {len(invalid_billing)} invalid patient_id references in fact_billing.",
    )

    for rejected_name, rejected_df in rejected_tables.items():
        add_result(
            f"rejected_records_{rejected_name}",
            "INFO",
            f"{len(rejected_df)} rejected records written for {rejected_name}.",
        )

    if not rejected_tables:
        add_result("rejected_records", "INFO", "No rejected records were generated.")

    return results


def run_validation_checks(
    clean_tables: dict[str, pd.DataFrame],
    rejected_tables: dict[str, pd.DataFrame],
    target_config_path: str | Path = TARGET_CONFIG_PATH,
) -> list[dict[str, Any]]:
    from sqlalchemy import text
    from src.utils.db import get_engine, get_schema_name

    results = build_in_memory_validation_results(clean_tables, rejected_tables)
    engine = get_engine(target_config_path)
    schema = get_schema_name(target_config_path)

    with engine.connect() as connection:
        for table_name, df in clean_tables.items():
            db_count = connection.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}")).scalar_one()
            results.append(
                {
                    "check_name": f"row_count_{table_name}",
                    "status": "PASS" if db_count == len(df) else "FAIL",
                    "details": (
                        f"Target row count matches for {table_name}: dataframe={len(df)}, database={db_count}"
                        if db_count == len(df)
                        else f"Target row count mismatch for {table_name}: dataframe={len(df)}, database={db_count}"
                    ),
                }
            )

    return results
