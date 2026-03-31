from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.config_loader import load_yaml_config


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def build_connection_string(config_path: str | Path) -> str:
    config = load_yaml_config(config_path)["database"]
    return (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )


def get_engine(config_path: str | Path) -> Engine:
    return create_engine(build_connection_string(config_path), future=True)


def execute_sql_file(engine: Engine, sql_file_path: str | Path) -> None:
    sql_text = Path(sql_file_path).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql_text.split(";") if statement.strip()]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def get_schema_name(config_path: str | Path) -> str:
    return load_yaml_config(config_path)["database"]["schema"]
