import pandas as pd
from src.utils.db import get_engine
from src.utils.logger import get_logger

LOGGER = get_logger()

SQL_QUERIES = {
    "patients": "SELECT * FROM legacy.legacy_patients",
    "appointments": "SELECT * FROM legacy.legacy_appointments",
    "billing_transactions": "SELECT * FROM legacy.legacy_billing_transactions",
}


def extract_postgres_data() -> dict[str, pd.DataFrame]:
    LOGGER.info("Extracting source data from legacy schema.")
    extracted_data: dict[str, pd.DataFrame] = {}

    engine = get_engine()

    with engine.connect() as connection:
        for dataset_name, query in SQL_QUERIES.items():
            LOGGER.info(f"Running extract query for {dataset_name}.")
            extracted_data[dataset_name] = pd.read_sql(query, connection)

    return extracted_data