from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.utils.db import get_engine, get_schema_name


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.yaml"
TARGET_CONFIG_PATH = PROJECT_ROOT / "config" / "target_config.yaml"


SOURCE_TO_TARGET_MAP = {
    "patients": "dim_patient",
    "appointments": "fact_appointment",
    "billing_transactions": "fact_billing",
}


def run_reconciliation(
    source_data: dict[str, pd.DataFrame],
    clean_tables: dict[str, pd.DataFrame],
    source_config_path: str | Path = SOURCE_CONFIG_PATH,
    target_config_path: str | Path = TARGET_CONFIG_PATH,
) -> list[dict[str, Any]]:
    engine = get_engine(target_config_path)
    target_schema = get_schema_name(target_config_path)
    results: list[dict[str, Any]] = []

    with engine.connect() as connection:
        for source_name, target_name in SOURCE_TO_TARGET_MAP.items():
            source_count = len(source_data[source_name])
            clean_count = len(clean_tables[target_name])
            target_count = connection.execute(
                text(f"SELECT COUNT(*) FROM {target_schema}.{target_name}")
            ).scalar_one()

            status = "PASS" if clean_count == target_count else "FAIL"
            results.append(
                {
                    "source_dataset": source_name,
                    "target_table": target_name,
                    "source_count": source_count,
                    "clean_count": clean_count,
                    "target_count": target_count,
                    "status": status,
                    "details": (
                        f"Source={source_count}, clean={clean_count}, target={target_count}. "
                        f"Difference between source and clean may be expected if rows were rejected."
                    ),
                }
            )

    return results
