import json
import psycopg2
import numpy as np
from dagster import asset, OpExecutionContext, Field, StringSource

from etl_wo.config.config import config as env_config

organizations = env_config.get("organizations", {})


@asset(
    config_schema={
        "organization": Field(StringSource, default_value="local", is_required=False)
    }
)
def talon_load_normal(context: OpExecutionContext, talon_transform: dict):
    """Загружает данные для обычных талонов в таблицу load_data_talons."""
    payload = talon_transform.get("normal", {})
    table_name = payload.get("table_name")
    data = payload.get("data")

    org = context.op_config.get("organization", "local")
    if org not in organizations:
        raise ValueError(f"База {org} не найдена в db_connections.json")
    db_config = organizations[org]

    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    cursor = conn.cursor()

    if data is None or data.empty:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        text_value = f"ℹ️ Нет данных для таблицы {table_name} (обычные талоны). Итоговое число строк: {final_count}"
        context.log.info(text_value)
        print(text_value)
        return {"table_name": table_name, "status": "skipped"}

    data.fillna("-", inplace=True)
    for _, row in data.iterrows():
        sql = f"""
        INSERT INTO {table_name} ({', '.join(data.columns)})
        VALUES ({', '.join(['%s'] * len(data.columns))})
        ON CONFLICT (talon, source)
        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in data.columns if col not in ('talon', 'source')])};
        """
        cursor.execute(sql, tuple(row))
    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    final_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    context.log.info(f"📤 Обычные талоны загружены в {table_name}. Итоговое число строк: {final_count}")
    return {"table_name": table_name, "status": "success"}
