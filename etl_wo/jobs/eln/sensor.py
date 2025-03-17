import os
import time
import json
import fnmatch

from dagster import sensor, RunRequest, SkipReason
from etl_wo.jobs.eln import job_eln
from etl_wo.jobs.eln.flow_config import DATA_FOLDER, MAPPING_FILE, TABLE_NAME

# Порог времени, чтобы считать, что файл полностью загружен (в секундах)
MIN_FILE_AGE_SECONDS = 60


def _load_state(context) -> dict:
    """Загружаем из cursor словарь вида {filename: run_key}."""
    if context.cursor:
        return json.loads(context.cursor)
    return {}


def _save_state(context, state: dict):
    """Сохраняем словарь {filename: run_key} в cursor сенсора."""
    context.update_cursor(json.dumps(state))


@sensor(job=job_eln)
def eln_folder_monitor_sensor(context):
    """
    Сенсор для мониторинга папки DATA_FOLDER:
      - Не допускает повторных запусков для файла, если предыдущий Run ещё идёт.
      - Если предыдущий Run успешен — удаляет файл.
      - Если предыдущий Run завершился ошибкой — перезапускает.
    """

    sensor_state = _load_state(context)  # {filename: run_key}

    # 1. Проверка файла маппинга
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

    # 2. Формируем шаблон (например, "ЛН_*.csv")
    file_pattern = table_config.get("file", {}).get("file_pattern", "")
    file_format = table_config.get("file", {}).get("file_format", "")
    valid_pattern = f"{file_pattern}.{file_format}"

    # 3. Проверяем папку
    if not os.path.exists(DATA_FOLDER):
        context.log.info(f"❌ Папка {DATA_FOLDER} не найдена.")
        yield SkipReason("Data folder not found.")
        return

    files = os.listdir(DATA_FOLDER)
    if not files:
        context.log.info("📂 Папка DATA_FOLDER пуста, пропускаем тик.")
        yield SkipReason("Нет файлов в папке.")
        return

    now = time.time()
    valid_files = []
    invalid_files = []

    # 4. Разделяем файлы на валидные и невалидные
    for file in files:
        file_path = os.path.join(DATA_FOLDER, file)
        if fnmatch.fnmatch(file, valid_pattern):
            mod_time = os.path.getmtime(file_path)
            age = now - mod_time
            if age >= MIN_FILE_AGE_SECONDS:
                valid_files.append(file)
            else:
                context.log.info(f"Файл {file} ещё не полностью загружен (возраст {age:.0f} сек.).")
        else:
            invalid_files.append(file)

    # 5. Удаляем невалидные файлы
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

    # 6. Для каждого валидного файла проверяем, не обрабатывается ли он уже
    for file in valid_files:
        file_path = os.path.join(DATA_FOLDER, file)
        existing_run_key = sensor_state.get(file)

        if existing_run_key:
            # Ищем запущенный Run с таким run_key
            runs = context.instance.get_runs()
            matching_run = None
            for r in runs:
                if r.run_key == existing_run_key:
                    matching_run = r
                    break

            if matching_run:
                # Проверяем методы r.is_finished, r.is_success, r.is_failure
                if not matching_run.is_finished:
                    # Запуск ещё идёт
                    context.log.info(f"Файл {file} уже обрабатывается (run_key={existing_run_key}), статус={matching_run.status}. Пропускаем.")
                    continue
                else:
                    # Запуск завершён
                    if matching_run.is_success:
                        # Удаляем файл и запись из состояния
                        try:
                            os.remove(file_path)
                            context.log.info(f"Файл {file} успешно обработан и удалён.")
                        except Exception as e:
                            context.log.error(f"Не удалось удалить файл {file_path}: {e}")
                        del sensor_state[file]
                        continue
                    elif matching_run.is_failure:
                        context.log.warning(f"Файл {file} завершился ошибкой. Перезапускаем.")
                        del sensor_state[file]
                    else:
                        # Завершился, но ни успех, ни ошибка (например, CANCELED)
                        context.log.warning(f"Файл {file} завершился статусом {matching_run.status}, считаем ошибкой. Перезапускаем.")
                        del sensor_state[file]
            else:
                # Не нашли Run — возможно, run_key устарел, пробуем заново
                context.log.warning(f"Не найден Run c run_key={existing_run_key}. Перезапуск для файла {file}.")
                del sensor_state[file]

        # Если мы дошли сюда, значит для этого файла нет "активного" запуска
        if file not in sensor_state:
            # Создаём новый Run
            new_run_key = f"{file}-{int(time.time())}"
            context.log.info(f"Запуск процесса обновления для файла {file} c run_key={new_run_key}.")

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

            yield RunRequest(run_key=new_run_key, run_config=run_config)
            sensor_state[file] = new_run_key

    # 7. Сохраняем обновлённое состояние
    _save_state(context, sensor_state)
