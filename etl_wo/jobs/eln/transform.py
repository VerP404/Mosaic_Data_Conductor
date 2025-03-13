import os
import json
import pandas as pd
from dagster import asset, OpExecutionContext


@asset
def sick_leave_transform(context: OpExecutionContext, sick_leave_extract: dict) -> dict:
    """
    Трансформирует данные для таблицы load_data_sick_leave_sheets.
    - Применяет маппинг колонок согласно настройкам из mapping.json.
    - Добавляет отсутствующие колонки дефолтным значением "-".
    """
    table_key = sick_leave_extract.get("table_name", "load_data_sick_leave_sheets")
    df = sick_leave_extract.get("data", None)
    if df is None:
        error_msg = f"❌ Нет данных для трансформации таблицы {table_key}."
        context.log.info(error_msg)
        raise ValueError(error_msg)

    # Загружаем настройки маппинга из mapping.json
    mapping_path = os.path.join("etl_wo", "config", "mapping.json")
    with open(mapping_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    table_config = mappings["tables"].get(table_key, {})
    column_mapping = table_config.get("mapping_fields", {})


    # Переименовываем столбцы согласно маппингу
    df = df.rename(columns=column_mapping)

    # Удаляем строки, где поле "number" отсутствует или NaN
    df = df.dropna(subset=["number"])

    # Удаляем столбцы, имена которых начинаются с "Unnamed"
    df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

    # Добавляем недостающие столбцы, если они указаны в маппинге
    mapped_columns = list(column_mapping.values())
    for col in mapped_columns:
        if col not in df.columns:
            df[col] = "-"

    text_value = f"🔄 Трансформация для {table_key} завершена. Загружено строк: {len(df)}"
    context.log.info(text_value)
    return {"table_name": table_key, "data": df}
