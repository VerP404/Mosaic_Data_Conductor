import json
import pandas as pd
from dagster import asset, Field, String, OpExecutionContext, AssetIn

from etl_wo.common.connect_db import connect_to_db
from etl_wo.config.config import ORGANIZATIONS
from etl_wo.jobs.job1.flow_config import MAPPING_FILE, TABLE_NAME, NORMAL_TABLE, COMPLEX_TABLE

@asset(
    config_schema={
        "mapping_file": Field(String, default_value=MAPPING_FILE),
        "table_name": Field(String, default_value=TABLE_NAME),
        "normal_table": Field(String, default_value=NORMAL_TABLE),
        "complex_table": Field(String, default_value=COMPLEX_TABLE),
        # Можно добавить параметр для выбора базы, если требуется:
        "db_alias": Field(String, default_value="default"),
    },
    ins={"talon_extract2": AssetIn()}
)
def talon_transform2(context: OpExecutionContext, talon_extract2: dict) -> dict:
    """
    Трансформация данных:
      1. Загружает настройки маппинга из mapping.json и переименовывает столбцы.
      2. Получает список обязательных столбцов из схемы таблицы базы данных.
      3. Если в DataFrame отсутствуют обязательные столбцы, добавляет их со значением дефолта "-".
      4. Добавляет столбец "is_complex" (по умолчанию False) и определяет комплексные записи
         (если по паре (talon, source) найдено более одной строки).
      5. Делит данные на два DataFrame: normal и complex.
      6. Возвращает словарь с ключами "normal" и "complex", где для каждого указывается имя таблицы для загрузки.
    """
    # Получаем параметры конфигурации
    config = context.op_config
    mapping_file = config["mapping_file"]
    table_name = config["table_name"]
    normal_table = config["normal_table"]
    complex_table = config["complex_table"]
    db_alias = config["db_alias"]

    # Извлекаем DataFrame из предыдущего этапа
    df = talon_extract2.get("data")
    if df is None:
        context.log.error("❌ Ошибка: Нет данных для трансформации!")
        raise ValueError("Нет данных для трансформации.")

    # Загружаем маппинг и переименовываем столбцы
    with open(mapping_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)
    table_config = mappings.get("tables", {}).get(table_name, {})
    column_mapping = table_config.get("mapping_fields", {})

    # Приводим столбцы к требуемому виду
    df = df.rename(columns=column_mapping)
    df = df[list(column_mapping.values())]

    # Получаем список обязательных столбцов (только для varchar) из схемы таблицы БД
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
        context.log.error(f"❌ Не удалось получить список столбцов для таблицы {table_name} из базы данных.")
        raise ValueError(f"Нет столбцов для таблицы {table_name}.")

    context.log.info(f"✅ Обязательные столбцы из БД (varchar): {db_columns}")

    # Заполняем отсутствующие обязательные столбцы дефолтным значением
    for col in db_columns:
        if col not in df.columns:
            df[col] = "-"

    # Добавляем столбец is_complex по умолчанию
    df["is_complex"] = False

    # Определяем комплексные записи: если по паре (talon, source) найдено более одной записи,
    # помечаем их как комплексные
    grouped = df.groupby(["talon", "source"])
    for (talon, source), group in grouped:
        if len(group) > 1:
            df.loc[group.index, "is_complex"] = True

    # Делим DataFrame на два: обычные и комплексные записи
    normal_df = df[df["is_complex"] == False].copy()
    complex_df = df[df["is_complex"] == True].copy()

    normal_count = len(normal_df)
    complex_count = len(complex_df)
    context.log.info(
        f"🔄 Трансформация завершена. Всего строк: {len(df)}. "
        f"Обычных: {normal_count}. Комплексных: {complex_count}."
    )

    return {
        "normal": {"table_name": normal_table, "data": normal_df},
        "complex": {"table_name": complex_table, "data": complex_df}
    }
