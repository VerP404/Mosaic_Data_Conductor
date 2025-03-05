import json
import psycopg2
from sqlalchemy import create_engine

from etl_wo.config.config import config as env_config

organizations = env_config.get("organizations", {})


def connect_to_db(org="local", table_name=None):
    """
    Подключается к базе данных, проверяет наличие таблицы в .env.
    :param org: Название организации (по умолчанию "local")
    :param table_name: Название таблицы, которую проверяем
    :return: SQLAlchemy engine и psycopg2 соединение
    """
    if org not in organizations:
        raise ValueError(f"База {org} не найдена в .env")

    db_config = organizations[org]

    if table_name and table_name not in db_config["tables"]:
        raise ValueError(f"Таблица {table_name} не найдена в базе {org}")

    # Создаем SQLAlchemy engine
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )

    # Подключаемся через psycopg2
    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )

    print(f"✅ Подключение к базе {db_config['dbname']} успешно!")
    return engine, conn
