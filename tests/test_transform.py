import pandas as pd

from src.transform import transform_all_data


def test_transform_all_data_deduplicates_patients_and_rejects_invalid_children():
    source_data = {
        "patients": pd.DataFrame(
            {
                "legacy_patient_id": [1, 1, 2],
                "fname": ["john", "john", "jane"],
                "lname": ["doe", "doe", "smith"],
                "dob": ["1985-01-10", "1985-01-10", "1990-03-22"],
                "sex": ["M", "M", "F"],
                "email": ["JOHN@EMAIL.COM", "JOHN@EMAIL.COM", "JANE@EMAIL.COM"],
            }
        ),
        "appointments": pd.DataFrame(
            {
                "appointment_id": [1001, 1002],
                "patient_id": [1, 99],
                "appointment_date": ["2025-01-01 09:00:00", "2025-01-01 10:00:00"],
                "appointment_status": ["completed", "completed"],
                "provider_name": ["dr. brown", "dr. green"],
            }
        ),
        "billing_transactions": pd.DataFrame(
            {
                "billing_id": [5001],
                "patient_id": [99],
                "billing_date": ["2025-01-01"],
                "amount": ["123.45"],
                "procedure_code": ["exam01"],
            }
        ),
    }

    result = transform_all_data(source_data)
    clean_tables = result["clean_tables"]
    rejected_tables = result["rejected_tables"]

    assert len(clean_tables["dim_patient"]) == 2
    assert clean_tables["dim_patient"].iloc[0]["first_name"] == "John"
    assert "rejected_patients_duplicates" in rejected_tables
    assert "rejected_appointments_invalid_fk" in rejected_tables
    assert "rejected_billing_transactions_invalid_fk" in rejected_tables
