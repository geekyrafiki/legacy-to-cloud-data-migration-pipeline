from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.utils.config_loader import load_yaml_config
from src.utils.db import get_engine, get_schema_name


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIELD_MAPPINGS_PATH = PROJECT_ROOT / "config" / "field_mappings.yaml"
SOURCE_CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.yaml"


def extract_postgres_data(
    source_config_path: str | Path = SOURCE_CONFIG_PATH,
    field_mappings_path: str | Path = FIELD_MAPPINGS_PATH,
) -> dict[str, pd.DataFrame]:
    field_mappings = load_yaml_config(field_mappings_path)
    engine = get_engine(source_config_path)
    source_schema = get_schema_name(source_config_path)

    extracted_data: dict[str, pd.DataFrame] = {}

    with engine.connect() as connection:
        for dataset_name, dataset_config in field_mappings.items():
            source_table = dataset_config["source_table"]
            query = text(f"SELECT * FROM {source_schema}.{source_table}")
            extracted_data[dataset_name] = pd.read_sql(query, connection)

    return extracted_data
