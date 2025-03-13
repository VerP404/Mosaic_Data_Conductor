import os
import json
import pandas as pd
from dagster import OpExecutionContext


def universal_transform(
        context: OpExecutionContext,
        mapping_file: str,
        table_name: str,
        df: pd.DataFrame
) -> dict:
    """
    Универсальная функция для трансформации данных.

    Параметры:
      context: Dagster execution context.
      mapping_file: Путь к файлу mapping.json с настройками таблиц.
      table_name: Имя таблицы, для которой выполняется трансформация.
      df: DataFrame, полученный на этапе извлечения.

    Функция:
      1. Загружает настройки из mapping.json.
      2. Получает ожидаемые имена столбцов (mapping_fields) для указанной таблицы.
      3. Сверяет имена столбцов в DataFrame с ожидаемыми.
      4. Логгирует отсутствующие столбцы (если есть).
      5. Переименовывает столбцы в соответствии с mapping_fields.
      6. Возвращает словарь с ключами:
         - "table_name" (имя таблицы),
         - "data" (трансформированный DataFrame),
         - "missing_columns" (список отсутствующих столбцов).
    """

    # Проверяем наличие файла маппинга
    if not os.path.exists(mapping_file):
        context.log.error(f"❌ Файл маппинга {mapping_file} не найден.")
        raise FileNotFoundError(f"Файл маппинга {mapping_file} не найден.")

    # Загружаем настройки маппинга
    with open(mapping_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    table_config = mappings.get("tables", {}).get(table_name)
    if not table_config:
        context.log.error(f"❌ Настройки для таблицы '{table_name}' не найдены в {mapping_file}.")
        raise ValueError(f"Настройки для таблицы '{table_name}' не найдены.")

    mapping_fields = table_config.get("mapping_fields", {})

    # Сверяем имена столбцов в DataFrame с ожидаемыми
    missing_columns = [col for col in mapping_fields.keys() if col not in df.columns]
    if missing_columns:
        context.log.warning(f"⚠️ Отсутствуют следующие столбцы в данных: {missing_columns}")

    # Переименовываем только те столбцы, которые реально есть в DataFrame
    rename_dict = {orig: new for orig, new in mapping_fields.items() if orig in df.columns}
    transformed_df = df.rename(columns=rename_dict)

    context.log.info(f"✅ Итоговый список столбцов: {list(transformed_df.columns)}")

    return {
        "table_name": table_name,
        "data": transformed_df,
        "missing_columns": missing_columns
    }
