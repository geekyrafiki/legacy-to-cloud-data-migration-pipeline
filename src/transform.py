import pandas as pd
from src.transformations.patients_transform import transform_patients
from src.transformations.appointments_transform import transform_appointments
from src.transformations.billing_transform import transform_billing


def transform_all_data(source_data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    patients_df = transform_patients(source_data["patients"])
    appointments_df = transform_appointments(source_data["appointments"], patients_df)
    billing_df = transform_billing(source_data["billing_transactions"], patients_df)

    return {
        "dim_patient": patients_df,
        "fact_appointment": appointments_df,
        "fact_billing": billing_df,
    }