import os
import time
import json
import fnmatch

from dagster import sensor, RunRequest, SkipReason
from etl_wo.jobs.kvazar import job_eln

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
def kvazar_folder_monitor_sensor(context):
    """
    Сенсор для мониторинга папки DATA_FOLDER:
      - Не допускает повторных запусков для файла, если предыдущий Run ещё идёт.
      - Если предыдущий Run успешен — удаляет файл.
      - Если предыдущий Run завершился ошибкой — перезапускает.
    """
    sensor_config = context.sensor_config
    mapping_file = sensor_config["mapping_file"]
    data_folder = sensor_config["data_folder"]
    table_name = sensor_config["table_name"]
    sensor_state = _load_state(context)  # {filename: run_key}

    # 1. Проверяем наличие mapping.json
    if not os.path.exists(mapping_file):
        context.log.info(f"❌ Файл маппинга {mapping_file} не найден.")
        yield SkipReason("Mapping file not found.")
        return

    with open(mapping_file, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    table_config = mapping.get("tables", {}).get(table_name)
    if not table_config:
        context.log.info(f"❌ Настройки для таблицы '{table_name}' не найдены в {mapping_file}.")
        yield SkipReason("Mapping config for table not found.")
        return

    # 2. Формируем шаблон (например, "ЛН_*.csv")
    file_pattern = table_config.get("file", {}).get("file_pattern", "")
    file_format = table_config.get("file", {}).get("file_format", "")
    valid_pattern = f"{file_pattern}.{file_format}"

    # 3. Проверяем наличие папки с данными
    if not os.path.exists(data_folder):
        context.log.info(f"❌ Папка {data_folder} не найдена.")
        yield SkipReason("Data folder not found.")
        return

    files = os.listdir(data_folder)
    if not files:
        context.log.info(f"📂 Папка {data_folder} пуста, пропускаем тик.")
        yield SkipReason("Нет файлов в папке.")
        return

    now = time.time()
    valid_files = []
    invalid_files = []

    # 4. Разделяем файлы на валидные и невалидные
    for file in files:
        file_path = os.path.join(data_folder, file)
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
        file_path = os.path.join(data_folder, file)
        try:
            os.remove(file_path)
            context.log.info(f"Удалён невалидный файл: {file_path}")
        except Exception as e:
            context.log.error(f"Не удалось удалить файл {file_path}: {e}")

    if not valid_files:
        context.log.info("Нет валидных файлов для запуска обновления.")
        yield SkipReason("Нет валидных файлов.")
        return

    # 6. Для каждого валидного файла смотрим, не обрабатывается ли он уже
    for file in valid_files:
        file_path = os.path.join(data_folder, file)
        existing_run_key = sensor_state.get(file)

        if existing_run_key:
            # Ищем запущенный Run, у которого run.tags["dagster/run_key"] == existing_run_key
            runs = context.instance.get_runs()
            matching_run = None
            for r in runs:
                # !!! Вот здесь главное отличие: r.tags.get("dagster/run_key")
                if r.tags.get("dagster/run_key") == existing_run_key:
                    matching_run = r
                    break

            if matching_run:
                # Проверяем методы: is_finished, is_success, is_failure
                if not matching_run.is_finished:
                    # Запуск ещё идёт
                    context.log.info(f"Файл {file} уже обрабатывается (run_key={existing_run_key}), статус={matching_run.status}. Пропускаем.")
                    continue
                else:
                    # Запуск завершён
                    if matching_run.is_success:
                        # Успешно — удаляем файл, убираем запись из состояния
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
                        # Например, CANCELED или другие статусы
                        context.log.warning(f"Файл {file} завершился статусом {matching_run.status}, считаем ошибкой. Перезапускаем.")
                        del sensor_state[file]
            else:
                # Не нашли Run — возможно, run_key устарел
                context.log.warning(f"Не найден Run c run_key={existing_run_key}. Перезапуск для файла {file}.")
                del sensor_state[file]

        # Если здесь, значит либо файла не было в состоянии, либо мы его удалили
        if file not in sensor_state:
            new_run_key = f"{file}-{int(time.time())}"
            context.log.info(f"Запуск процесса обновления для файла {file} c run_key={new_run_key}.")

            run_config = {
                "ops": {
                    "kvazar_extract": {
                        "config": {
                            "data_folder": data_folder,
                            "mapping_file": mapping_file,
                            "table_name": table_name,
                        }
                    }
                }
            }

            yield RunRequest(run_key=new_run_key, run_config=run_config)
            sensor_state[file] = new_run_key

    # 7. Сохраняем обновлённое состояние
    _save_state(context, sensor_state)
