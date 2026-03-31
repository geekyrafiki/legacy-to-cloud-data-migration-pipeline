from sqlalchemy import text
from src.utils.db import get_engine
from src.utils.logger import get_logger

LOGGER = get_logger()


def create_target_tables(target_schema_sql_path: str) -> None:
    LOGGER.info("Creating target tables in warehouse schema.")
    engine = get_engine()

    with open(target_schema_sql_path, "r", encoding="utf-8") as file:
        schema_sql = file.read()

    with engine.begin() as connection:
        connection.execute(text(schema_sql))

    LOGGER.info("Target schema creation completed.")


def truncate_target_tables() -> None:
    LOGGER.info("Truncating warehouse target tables.")
    engine = get_engine()

    truncate_sql = """
    TRUNCATE TABLE
        warehouse.fact_billing,
        warehouse.fact_appointment,
        warehouse.dim_patient
    RESTART IDENTITY CASCADE;
    """

    with engine.begin() as connection:
        connection.execute(text(truncate_sql))

    LOGGER.info("Warehouse target tables truncated successfully.")


def load_dataframes_to_postgres(dataframes: dict[str, object]) -> None:
    LOGGER.info("Loading transformed data into warehouse schema.")
    engine = get_engine()

    load_order = [
        ("dim_patient", "dim_patient"),
        ("fact_appointment", "fact_appointment"),
        ("fact_billing", "fact_billing"),
    ]

    for dataframe_name, target_table in load_order:
        df = dataframes[dataframe_name]
        LOGGER.info(f"Appending {len(df)} rows into warehouse.{target_table}")

        df.to_sql(
            name=target_table,
            con=engine,
            schema="warehouse",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

    LOGGER.info("All transformed data loaded into warehouse schema.")