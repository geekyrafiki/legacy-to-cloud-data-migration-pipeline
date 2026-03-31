import pandas as pd

from src.validate import build_in_memory_validation_results


def test_in_memory_validation_passes_for_clean_tables():
    clean_tables = {
        "dim_patient": pd.DataFrame(
            {
                "patient_id": [1, 2],
                "first_name": ["John", "Jane"],
                "last_name": ["Doe", "Smith"],
                "date_of_birth": [None, None],
                "gender": ["Male", "Female"],
                "email": ["john@email.com", "jane@email.com"],
            }
        ),
        "fact_appointment": pd.DataFrame(
            {
                "appointment_id": [1001],
                "patient_id": [1],
                "appointment_date": [None],
                "appointment_status": ["Completed"],
                "provider_name": ["Dr. Brown"],
            }
        ),
        "fact_billing": pd.DataFrame(
            {
                "billing_id": [5001],
                "patient_id": [1],
                "billing_date": [None],
                "amount": [150.0],
                "procedure_code": ["EXAM01"],
            }
        ),
    }

    results = build_in_memory_validation_results(clean_tables, rejected_tables={})
    statuses = {row["check_name"]: row["status"] for row in results}

    assert statuses["dim_patient_null_patient_id"] == "PASS"
    assert statuses["dim_patient_duplicate_patient_id"] == "PASS"
    assert statuses["fact_appointment_invalid_patient_refs"] == "PASS"
    assert statuses["fact_billing_invalid_patient_refs"] == "PASS"
