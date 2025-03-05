import pandas as pd
import json
import os
import fnmatch
from dagster import asset, Output, OpExecutionContext

# Пути к файлам
MAPPING_PATH = "etl_wo/config/mapping.json"
DATA_PATH = "etl_wo/data/"

# Загружаем mapping.json
with open(MAPPING_PATH, "r", encoding="utf-8") as f:
    mappings = json.load(f)


@asset
def extract(context: OpExecutionContext, check_db: dict, download_oms_file: str) -> dict:
    """
    Извлекает CSV-файл из папки data.
    Для каждой таблицы из mapping.json ищет файлы по шаблону и выбирает последний (отсортированный по имени).
    """
    db_tables = check_db["tables"]

    data_files = os.listdir(DATA_PATH)
    if not data_files:
        raise FileNotFoundError("❌ Нет доступных файлов в папке data/")

    matched_table = None
    matched_file = None

    for table_name, config in mappings["tables"].items():
        file_pattern = config.get("file", {}).get("file_pattern", "")
        file_format = config.get("file", {}).get("file_format", "")

        # Проверяем, что таблица зарегистрирована в настройках БД
        if table_name not in db_tables:
            text_value = f"⚠️ Таблица {table_name} есть в mapping.json, но отсутствует в БД."
            context.log.info(text_value)
            print(text_value)
            continue

        # Находим все файлы, соответствующие шаблону
        matching_files = [f for f in data_files if fnmatch.fnmatch(f, f"{file_pattern}.{file_format}")]
        if matching_files:
            # Выбираем последний файл из отсортированного списка
            matched_file = sorted(matching_files)[-1]
            matched_table = table_name
            break

    if not matched_table:
        raise ValueError("❌ Не найден файл, соответствующий таблице в mapping.json и имеющийся в БД")

    table_config = mappings["tables"][matched_table]
    file_path = os.path.join(DATA_PATH, matched_file)

    df = pd.read_csv(
        file_path,
        encoding=table_config["encoding"],
        delimiter=table_config["delimiter"]
    )
    text_value = f"📥 Загружено {len(df)} строк из {matched_file}"
    context.log.info(text_value)

    print(text_value)

    return {"table_name": matched_table, "data": df}
