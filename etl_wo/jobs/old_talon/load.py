import os

import psycopg2
import pandas as pd
from dagster import asset, OpExecutionContext, Field, StringSource
from etl_wo.config.config import config as env_config

organizations = env_config.get("organizations", {})


@asset(
    config_schema={
        "organization": Field(StringSource, default_value="default", is_required=False)
    }
)
def sink_leave_load(context: OpExecutionContext, sick_leave_transform: dict):
    """
    Загружает данные для таблицы load_data_sick_leave_sheets.
    Если данные отсутствуют, логирует и пропускает загрузку.
    """
    # Жёстко заданное имя таблицы
    table_name = "load_data_sick_leave_sheets"
    payload = sick_leave_transform.get("data")

    org = context.op_config.get("organization", "local")
    if org not in organizations:
        raise ValueError(f"База {org} не найдена в конфигурации.")
    db_config = organizations[org]

    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    cursor = conn.cursor()

    if payload is None or payload.empty:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        context.log.info(f"ℹ️ Нет данных для загрузки в таблицу {table_name}. Итоговое число строк: {final_count}")
        cursor.close()
        conn.close()
        return {"table_name": table_name, "status": "skipped"}

    payload.fillna("-", inplace=True)

    # Формируем запрос для вставки данных. Предполагается, что уникальное ограничение установлено на колонку "number"
    for _, row in payload.iterrows():
        cols = list(payload.columns)
        placeholders = ', '.join(['%s'] * len(cols))
        columns_joined = ', '.join(cols)
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in cols if col != "number"])
        sql = f"""
            INSERT INTO {table_name} ({columns_joined})
            VALUES ({placeholders})
            ON CONFLICT (number)
            DO UPDATE SET {update_clause};
        """
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    final_count = cursor.fetchone()[0]
    context.log.info(f"📤 Данные для {table_name} загружены. Итоговое число строк: {final_count}")
    cursor.close()
    conn.close()

    # Очистка папки с исходными файлами
    data_folder = os.path.join(os.getcwd(), "etl_wo", "data", "eln")
    if os.path.exists(data_folder):
        for f in os.listdir(data_folder):
            file_path = os.path.join(data_folder, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
        context.log.info(f"🧹 Папка {data_folder} успешно очищена.")
    else:
        context.log.info(f"Папка {data_folder} не найдена для очистки.")

    return {"table_name": table_name, "status": "success"}
