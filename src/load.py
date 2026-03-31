from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.utils.db import execute_sql_file, get_engine, get_schema_name


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET_CONFIG_PATH = PROJECT_ROOT / "config" / "target_config.yaml"
TARGET_SCHEMA_SQL = PROJECT_ROOT / "sql" / "target_schema.sql"
SOURCE_CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.yaml"
SOURCE_SCHEMA_SQL = PROJECT_ROOT / "sql" / "source_schema.sql"


TARGET_TABLE_ORDER = ["dim_patient", "fact_appointment", "fact_billing"]


def create_source_tables(
    source_config_path: str | Path = SOURCE_CONFIG_PATH,
    sql_file_path: str | Path = SOURCE_SCHEMA_SQL,
) -> None:
    execute_sql_file(get_engine(source_config_path), sql_file_path)



def create_target_tables(
    target_config_path: str | Path = TARGET_CONFIG_PATH,
    sql_file_path: str | Path = TARGET_SCHEMA_SQL,
) -> None:
    execute_sql_file(get_engine(target_config_path), sql_file_path)



def truncate_legacy_tables(source_config_path: str | Path = SOURCE_CONFIG_PATH) -> None:
    engine = get_engine(source_config_path)
    schema = get_schema_name(source_config_path)
    tables = ["legacy_patients", "legacy_appointments", "legacy_billing_transactions"]
    with engine.begin() as connection:
        for table in tables:
            connection.execute(text(f"TRUNCATE TABLE {schema}.{table}"))



def truncate_target_tables(target_config_path: str | Path = TARGET_CONFIG_PATH) -> None:
    engine = get_engine(target_config_path)
    schema = get_schema_name(target_config_path)
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {schema}.fact_appointment RESTART IDENTITY CASCADE"))
        connection.execute(text(f"TRUNCATE TABLE {schema}.fact_billing RESTART IDENTITY CASCADE"))
        connection.execute(text(f"TRUNCATE TABLE {schema}.dim_patient RESTART IDENTITY CASCADE"))



def load_dataframes_to_postgres(
    clean_tables: dict[str, pd.DataFrame],
    target_config_path: str | Path = TARGET_CONFIG_PATH,
) -> None:
    engine = get_engine(target_config_path)
    schema = get_schema_name(target_config_path)

    with engine.begin() as connection:
        for table_name in TARGET_TABLE_ORDER:
            df = clean_tables[table_name]
            df.to_sql(table_name, con=connection, schema=schema, if_exists="append", index=False)
