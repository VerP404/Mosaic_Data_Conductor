import os
import json
import pandas as pd
from dagster import asset, Field, String, OpExecutionContext, AssetIn
from etl_wo.common.connect_db import connect_to_db
from etl_wo.config.config import ORGANIZATIONS
from etl_wo.jobs.eln.flow_config import MAPPING_FILE, TABLE_NAME


@asset(
    config_schema={
        "mapping_file": Field(String, default_value=MAPPING_FILE),
        "table_name": Field(String, default_value=TABLE_NAME),
        "db_alias": Field(String, default_value="default"),
    },
    ins={"eln_extract": AssetIn()}  # меняем ключ с "sick_leave_extract" на "eln_extract"
)
def eln_transform(context: OpExecutionContext, eln_extract: dict) -> dict:
    """
    Универсальная трансформация данных для sick_leave:
      1. Загружает настройки маппинга из mapping.json и переименовывает столбцы.
      2. Извлекает обязательные столбцы (varchar) из схемы таблицы в базе данных.
      3. Добавляет отсутствующие обязательные столбцы со значением "-" по умолчанию.
      4. Возвращает единственный словарь с ключами "table_name" и "data".
    """
    # Получаем конфигурацию
    config = context.op_config
    mapping_file = config["mapping_file"]
    table_name = config["table_name"]
    db_alias = config["db_alias"]

    # Извлекаем DataFrame из предыдущего этапа
    df = eln_extract.get("data")
    if df is None:
        context.log.error("❌ Ошибка: Нет данных для трансформации!")
        raise ValueError("Нет данных для трансформации.")

    # Загружаем маппинг и переименовываем столбцы согласно mapping.json
    with open(mapping_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)
    table_config = mappings.get("tables", {}).get(table_name, {})
    column_mapping = table_config.get("mapping_fields", {})

    # Переименовываем столбцы
    df = df.rename(columns=column_mapping)
    # Ограничиваем DataFrame только колонками из маппинга
    df = df[list(column_mapping.values())]

    # Получаем обязательные столбцы (varchar) из схемы таблицы в базе данных
    engine, conn = connect_to_db(db_alias=db_alias, organization=ORGANIZATIONS, context=context)
    sql = f"""
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = '{table_name}' 
        AND data_type = 'character varying';
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        db_columns = [row[0] for row in cursor.fetchall()]
    conn.close()

    if not db_columns:
        context.log.error(f"❌ Не удалось получить список обязательных столбцов для таблицы {table_name}.")
        raise ValueError(f"Нет обязательных столбцов для таблицы {table_name}.")

    context.log.info(f"✅ Обязательные столбцы из БД (varchar): {db_columns}")

    # Заполняем отсутствующие обязательные столбцы дефолтным значением "-"
    for col in db_columns:
        if col not in df.columns:
            df[col] = "-"

    context.log.info(f"🔄 Трансформация для {table_name} завершена. Всего строк: {len(df)}")
    return {"table_name": table_name, "data": df}
