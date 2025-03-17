import os
import time
import json
import fnmatch
from dagster import sensor, RunRequest, SkipReason

from etl_wo.jobs.eln import job_eln
from etl_wo.jobs.eln.flow_config import DATA_FOLDER, MAPPING_FILE, TABLE_NAME

# Порог времени, чтобы считать, что файл полностью загружен (в секундах)
MIN_FILE_AGE_SECONDS = 60

@sensor(job=job_eln)
def eln_folder_monitor_sensor(context):
    """
    Сенсор для мониторинга папки DATA_FOLDER.

    1. Загружает mapping.json для получения настроек file_pattern и file_format для TABLE_NAME.
    2. Проверяет наличие файлов в папке DATA_FOLDER.
    3. Если файл соответствует шаблону, дополнительно проверяет, что файл не менялся в течение MIN_FILE_AGE_SECONDS.
    4. Если файл не соответствует шаблону – удаляет его.
    5. Для каждого валидного файла инициирует запуск джобы (без run_key, чтобы запускался каждый раз).
    """
    # Проверяем наличие файла mapping.json
    if not os.path.exists(MAPPING_FILE):
        context.log.info(f"❌ Файл маппинга {MAPPING_FILE} не найден.")
        yield SkipReason("Mapping file not found.")
        return

    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    table_config = mapping.get("tables", {}).get(TABLE_NAME)
    if not table_config:
        context.log.info(f"❌ Настройки для таблицы '{TABLE_NAME}' не найдены в {MAPPING_FILE}.")
        yield SkipReason("Mapping config for table not found.")
        return

    file_pattern = table_config.get("file", {}).get("file_pattern", "")
    file_format = table_config.get("file", {}).get("file_format", "")
    # Собираем шаблон вида "pattern.format" (например, "ЛН_*.csv")
    valid_pattern = f"{file_pattern}.{file_format}"

    # Проверяем наличие папки с данными
    if not os.path.exists(DATA_FOLDER):
        context.log.info(f"❌ Папка {DATA_FOLDER} не найдена.")
        yield SkipReason("Data folder not found.")
        return

    files = os.listdir(DATA_FOLDER)
    if not files:
        context.log.info("📂 Папка DATA_FOLDER пуста, пропускаем тик.")
        yield SkipReason("Нет файлов в папке.")
        return

    valid_files = []
    invalid_files = []
    now = time.time()

    for file in files:
        file_path = os.path.join(DATA_FOLDER, file)
        if fnmatch.fnmatch(file, valid_pattern):
            # Проверяем, что файл не менялся в течение MIN_FILE_AGE_SECONDS
            mod_time = os.path.getmtime(file_path)
            age = now - mod_time
            if age >= MIN_FILE_AGE_SECONDS:
                valid_files.append(file)
            else:
                context.log.info(f"Файл {file} еще не полностью загружен (возраст {age:.0f} сек.).")
        else:
            invalid_files.append(file)

    # Удаляем невалидные файлы
    for file in invalid_files:
        file_path = os.path.join(DATA_FOLDER, file)
        try:
            os.remove(file_path)
            context.log.info(f"Удалён невалидный файл: {file_path}")
        except Exception as e:
            context.log.error(f"Не удалось удалить файл {file_path}: {e}")

    if not valid_files:
        context.log.info("Нет валидных файлов для запуска обновления.")
        yield SkipReason("Нет валидных файлов.")
        return

    # Для каждого валидного файла формируем RunRequest
    for file in valid_files:
        file_path = os.path.join(DATA_FOLDER, file)
        context.log.info(f"Запуск процесса обновления для файла: {file}")

        # Конфигурация для запуска джобы
        run_config = {
            "ops": {
                "eln_extract": {
                    "config": {
                        "data_folder": DATA_FOLDER,
                        "mapping_file": MAPPING_FILE,
                        "table_name": TABLE_NAME,
                    }
                }
            }
        }
        # ВАЖНО: не указываем run_key, чтобы Dagster не блокировал повторные запуски
        yield RunRequest(run_config=run_config)
