from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.config_loader import load_yaml_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIELD_MAPPINGS_PATH = PROJECT_ROOT / "config" / "field_mappings.yaml"


GENDER_MAP = {
    "m": "Male",
    "f": "Female",
    "male": "Male",
    "female": "Female",
}


def _clean_patients(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed["first_name"] = transformed["first_name"].astype(str).str.strip().str.title()
    transformed["last_name"] = transformed["last_name"].astype(str).str.strip().str.title()
    transformed["email"] = transformed["email"].astype(str).str.strip().str.lower()
    transformed["gender"] = (
        transformed["gender"].astype(str).str.strip().str.lower().map(GENDER_MAP).fillna("Unknown")
    )
    transformed["date_of_birth"] = pd.to_datetime(
        transformed["date_of_birth"], errors="coerce"
    ).dt.date
    transformed["patient_id"] = pd.to_numeric(transformed["patient_id"], errors="coerce")
    return transformed


def _clean_appointments(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed["appointment_id"] = pd.to_numeric(transformed["appointment_id"], errors="coerce")
    transformed["patient_id"] = pd.to_numeric(transformed["patient_id"], errors="coerce")
    transformed["appointment_date"] = pd.to_datetime(
        transformed["appointment_date"], errors="coerce"
    )
    transformed["appointment_status"] = (
        transformed["appointment_status"].astype(str).str.strip().str.title()
    )
    transformed["provider_name"] = transformed["provider_name"].astype(str).str.strip().str.title()
    return transformed


def _clean_billing(df: pd.DataFrame) -> pd.DataFrame:
    transformed = df.copy()
    transformed["billing_id"] = pd.to_numeric(transformed["billing_id"], errors="coerce")
    transformed["patient_id"] = pd.to_numeric(transformed["patient_id"], errors="coerce")
    transformed["billing_date"] = pd.to_datetime(transformed["billing_date"], errors="coerce").dt.date
    transformed["amount"] = pd.to_numeric(transformed["amount"], errors="coerce")
    transformed["procedure_code"] = transformed["procedure_code"].astype(str).str.strip().str.upper()
    return transformed


def _rename_by_mapping(df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
    renamed = df.rename(columns=column_mapping).copy()
    expected_columns = list(column_mapping.values())
    return renamed[expected_columns]


def _build_rejected_rows(df: pd.DataFrame, reason: str) -> pd.DataFrame:
    rejected = df.copy()
    rejected["rejection_reason"] = reason
    return rejected


def transform_all_data(
    source_data: dict[str, pd.DataFrame],
    field_mappings_path: str | Path = FIELD_MAPPINGS_PATH,
) -> dict[str, Any]:
    mappings = load_yaml_config(field_mappings_path)
    transformed_tables: dict[str, pd.DataFrame] = {}
    rejected_tables: dict[str, pd.DataFrame] = {}

    patients_cfg = mappings["patients"]
    patients_df = _rename_by_mapping(source_data["patients"], patients_cfg["column_mapping"])
    patients_df = _clean_patients(patients_df)

    missing_patient_key = patients_df[patients_df["patient_id"].isna()]
    if not missing_patient_key.empty:
        rejected_tables["rejected_patients_missing_key"] = _build_rejected_rows(
            missing_patient_key, "Missing patient_id"
        )
    patients_df = patients_df[patients_df["patient_id"].notna()].copy()
    patients_df["patient_id"] = patients_df["patient_id"].astype(int)

    duplicate_patients = patients_df[patients_df.duplicated(subset=["patient_id"], keep="first")]
    if not duplicate_patients.empty:
        rejected_tables["rejected_patients_duplicates"] = _build_rejected_rows(
            duplicate_patients, "Duplicate patient_id"
        )
    patients_df = patients_df.drop_duplicates(subset=["patient_id"], keep="first")
    transformed_tables[patients_cfg["target_table"]] = patients_df

    valid_patient_ids = set(patients_df["patient_id"].tolist())

    for dataset_name in ("appointments", "billing_transactions"):
        dataset_cfg = mappings[dataset_name]
        transformed = _rename_by_mapping(source_data[dataset_name], dataset_cfg["column_mapping"])

        if dataset_name == "appointments":
            transformed = _clean_appointments(transformed)
            key_name = "appointment_id"
        else:
            transformed = _clean_billing(transformed)
            key_name = "billing_id"

        missing_required_mask = transformed[key_name].isna() | transformed["patient_id"].isna()
        missing_required = transformed[missing_required_mask]
        if not missing_required.empty:
            rejected_tables[f"rejected_{dataset_name}_missing_required"] = _build_rejected_rows(
                missing_required, f"Missing required column values for {dataset_name}"
            )
        transformed = transformed[~missing_required_mask].copy()
        transformed[key_name] = transformed[key_name].astype(int)
        transformed["patient_id"] = transformed["patient_id"].astype(int)

        duplicates = transformed[transformed.duplicated(subset=[key_name], keep="first")]
        if not duplicates.empty:
            rejected_tables[f"rejected_{dataset_name}_duplicates"] = _build_rejected_rows(
                duplicates, f"Duplicate {key_name}"
            )
        transformed = transformed.drop_duplicates(subset=[key_name], keep="first")

        invalid_fk = transformed[~transformed["patient_id"].isin(valid_patient_ids)]
        if not invalid_fk.empty:
            rejected_tables[f"rejected_{dataset_name}_invalid_fk"] = _build_rejected_rows(
                invalid_fk, "Invalid patient_id reference"
            )
        transformed = transformed[transformed["patient_id"].isin(valid_patient_ids)].copy()

        transformed_tables[dataset_cfg["target_table"]] = transformed

    return {
        "clean_tables": transformed_tables,
        "rejected_tables": rejected_tables,
    }
