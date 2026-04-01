import pandas as pd


def transform_all_data(source_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    patients_df = source_data["patients"].copy()
    appointments_df = source_data["appointments"].copy()
    billing_df = source_data["billing_transactions"].copy()

    # -------------------------
    # Transform patients
    # -------------------------
    patients_df.columns = [col.strip().lower() for col in patients_df.columns]

    patients_df = patients_df.rename(columns={
        "legacy_patient_id": "patient_id",
        "fname": "first_name",
        "lname": "last_name",
        "dob": "date_of_birth",
        "sex": "gender"
    })

    patients_df["first_name"] = patients_df["first_name"].astype(str).str.strip().str.title()
    patients_df["last_name"] = patients_df["last_name"].astype(str).str.strip().str.title()
    patients_df["email"] = patients_df["email"].astype(str).str.strip().str.lower()

    patients_df["gender"] = patients_df["gender"].replace({
        "M": "Male",
        "F": "Female",
        "m": "Male",
        "f": "Female"
    })

    patients_df["date_of_birth"] = pd.to_datetime(
        patients_df["date_of_birth"], errors="coerce"
    ).dt.date

    rejected_patients_duplicates = patients_df[
        patients_df.duplicated(subset=["patient_id"], keep="first")
    ].copy()

    patients_df = patients_df.drop_duplicates(subset=["patient_id"], keep="first")

    dim_patient = patients_df[[
        "patient_id",
        "first_name",
        "last_name",
        "date_of_birth",
        "gender",
        "email"
    ]].copy()

    valid_patient_ids = set(dim_patient["patient_id"].tolist())

    # -------------------------
    # Transform appointments
    # -------------------------
    appointments_df.columns = [col.strip().lower() for col in appointments_df.columns]

    appointments_df["appointment_date"] = pd.to_datetime(
        appointments_df["appointment_date"], errors="coerce"
    )

    appointments_df["appointment_status"] = (
        appointments_df["appointment_status"].astype(str).str.strip().str.title()
    )
    appointments_df["provider_name"] = (
        appointments_df["provider_name"].astype(str).str.strip().str.title()
    )

    rejected_appointments = appointments_df[
        ~appointments_df["patient_id"].isin(valid_patient_ids)
    ].copy()

    appointments_df = appointments_df[
        appointments_df["patient_id"].isin(valid_patient_ids)
    ].copy()

    appointments_df = appointments_df.drop_duplicates(subset=["appointment_id"], keep="first")

    fact_appointment = appointments_df[[
        "appointment_id",
        "patient_id",
        "appointment_date",
        "appointment_status",
        "provider_name"
    ]].copy()

    # -------------------------
    # Transform billing
    # -------------------------
    billing_df.columns = [col.strip().lower() for col in billing_df.columns]

    billing_df["billing_date"] = pd.to_datetime(
        billing_df["billing_date"], errors="coerce"
    ).dt.date

    billing_df["amount"] = pd.to_numeric(
        billing_df["amount"], errors="coerce"
    ).fillna(0.0)

    billing_df["procedure_code"] = (
        billing_df["procedure_code"].astype(str).str.strip().str.upper()
    )

    rejected_billing = billing_df[
        ~billing_df["patient_id"].isin(valid_patient_ids)
    ].copy()

    billing_df = billing_df[
        billing_df["patient_id"].isin(valid_patient_ids)
    ].copy()

    billing_df = billing_df.drop_duplicates(subset=["billing_id"], keep="first")

    fact_billing = billing_df[[
        "billing_id",
        "patient_id",
        "billing_date",
        "amount",
        "procedure_code"
    ]].copy()

    clean_tables = {
        "dim_patient": dim_patient,
        "fact_appointment": fact_appointment,
        "fact_billing": fact_billing,
    }

    rejected_tables = {
        "rejected_patients_duplicates": rejected_patients_duplicates,
        "rejected_appointments_invalid_fk": rejected_appointments,
        "rejected_billing_transactions_invalid_fk": rejected_billing,
    }

    return {
        "clean_tables": clean_tables,
        "rejected_tables": rejected_tables,
        "dim_patient": dim_patient,
        "fact_appointment": fact_appointment,
        "fact_billing": fact_billing,
    }