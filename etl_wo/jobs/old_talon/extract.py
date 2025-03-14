import os
import json
import fnmatch
import pandas as pd
from dagster import asset, OpExecutionContext

@asset
def old_talon_extract(context: OpExecutionContext) -> dict:
    """
    Извлекает CSV-файл из папки etl_wo/data/talon для таблицы load_data_sick_leave_sheets.
    Использует настройки из mapping.json для определения шаблона файла, кодировки и разделителя.
    """
    # Определяем пути к файлам
    mapping_path = os.path.join("etl_wo", "config", "mapping.json")
    data_folder = os.path.join("etl_wo", "data", "talon")

    # Загружаем настройки маппинга
    with open(mapping_path, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    table_key = "data_loader_omsdata"
    table_config = mappings["tables"].get(table_key)
    if not table_config:
        context.log.info(f"❌ Не найдены настройки для таблицы {table_key} в mapping.json")
        raise ValueError(f"❌ Не найдены настройки для таблицы {table_key} в mapping.json")

    # Получаем параметры поиска файла
    file_pattern = table_config.get("file", {}).get("file_pattern", "")
    file_format = table_config.get("file", {}).get("file_format", "")

    if not os.path.exists(data_folder):
        context.log.info(f"❌ Папка {data_folder} не найдена.")
        raise FileNotFoundError(f"❌ Папка {data_folder} не найдена.")

    data_files = os.listdir(data_folder)
    if not data_files:
        context.log.info(f"❌ Нет файлов в папке {data_folder}.")
        raise FileNotFoundError(f"❌ Нет файлов в папке {data_folder}.")

    # Находим файлы, соответствующие шаблону (например, "ЛН_*" + ".csv")
    matching_files = [f for f in data_files if fnmatch.fnmatch(f, f"{file_pattern}.{file_format}")]
    if not matching_files:
        context.log.info(f"❌ Не найдено файлов по шаблону {file_pattern}.{file_format} в {data_folder}.")
        raise ValueError(f"❌ Не найдено файлов по шаблону {file_pattern}.{file_format} в {data_folder}.")

    # Выбираем последний файл (отсортированный по имени)
    matched_file = sorted(matching_files)[-1]
    file_path = os.path.join(data_folder, matched_file)

    # Читаем CSV с использованием указанных в настройках параметров
    df = pd.read_csv(
        file_path,
        encoding=table_config.get("encoding", "utf-8"),
        delimiter=table_config.get("delimiter", ","),
        dtype=str
    )

    context.log.info(f"📥 Загружено {len(df)} строк из {matched_file}")
    return {"table_name": table_key, "data": df}
