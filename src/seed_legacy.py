from pathlib import Path

import pandas as pd

from src.load import create_source_tables, truncate_legacy_tables
from src.utils.db import get_engine, get_schema_name
from src.utils.logger import get_logger


LOGGER = get_logger()
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SOURCE_CONFIG_PATH = PROJECT_ROOT / "config" / "source_config.yaml"


SOURCE_FILES = {
    "legacy_patients": RAW_DIR / "patients.csv",
    "legacy_appointments": RAW_DIR / "appointments.csv",
    "legacy_billing_transactions": RAW_DIR / "billing_transactions.csv",
}


def seed_legacy_tables() -> None:
    LOGGER.info("Creating legacy schema and tables.")
    create_source_tables(SOURCE_CONFIG_PATH)
    LOGGER.info("Clearing existing legacy data.")
    truncate_legacy_tables(SOURCE_CONFIG_PATH)

    engine = get_engine(SOURCE_CONFIG_PATH)
    schema = get_schema_name(SOURCE_CONFIG_PATH)

    with engine.begin() as connection:
        for table_name, file_path in SOURCE_FILES.items():
            LOGGER.info("Loading %s into %s.%s", file_path.name, schema, table_name)
            df = pd.read_csv(file_path)
            df.to_sql(table_name, con=connection, schema=schema, if_exists="append", index=False)

    LOGGER.info("Legacy seed completed successfully.")


if __name__ == "__main__":
    seed_legacy_tables()
