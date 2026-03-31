import os
from sqlalchemy import create_engine
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "target_config.yaml")


def load_yaml_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_engine():
    config = load_yaml_config(CONFIG_PATH)["database"]

    connection_string = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )

    return create_engine(connection_string)