import os
import time
import json
import fnmatch
from dagster import sensor, RunRequest, SkipReason, RunStatus, check
from etl_wo.jobs.eln import job_eln
from etl_wo.jobs.eln.flow_config import DATA_FOLDER, MAPPING_FILE, TABLE_NAME

# Порог времени, чтобы считать, что файл полностью загружен (в секундах)
MIN_FILE_AGE_SECONDS = 60

def _load_state(context) -> dict:
    """Загружает словарь filename -> run_id из cursor сенсора."""
    if context.cursor:
        return json.loads(context.cursor)
    return {}

def _save_state(context, state: dict):
    """Сохраняет словарь filename -> run_id в cursor сенсора."""
    context.update_cursor(json.dumps(state))

@sensor(job=job_eln)
def eln_folder_monitor_sensor(context):
    """
    Сенсор для мониторинга папки DATA_FOLDER с учётом состояния:
      1. Если файл уже обрабатывается (Run не завершён), повторно не запускаем.
      2. Если предыдущая обработка файла завершилась успехом - удаляем файл и чистим состояние.
      3. Если предыдущая обработка файла завершилась ошибкой - перезапускаем обработку.
    """

    # Загружаем текущее состояние сенсора (filename -> run_id)
    sensor_state = _load_state(context)

    # Проверяем наличие файла mapping.json
    if not os.path.exists(MAPPING_FILE):
        context.log.info(f"❌ Файл маппинга {MAPPING_FILE} не найден.")
        yield SkipReason("Mapping file not found.")
        return

    # Загружаем конфиг для таблицы
    import json
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    table_config = mapping.get("tables", {}).get(TABLE_NAME)
    if not table_config:
        context.log.info(f"❌ Настройки для таблицы '{TABLE_NAME}' не найдены в {MAPPING_FILE}.")
        yield SkipReason("Mapping config for table not found.")
        return

    file_pattern = table_config.get("file", {}).get("file_pattern", "")
    file_format = table_config.get("file", {}).get("file_format", "")
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

    now = time.time()
    valid_files = []
    invalid_files = []

    # Разделяем файлы на валидные (соответствуют шаблону) и невалидные
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

    # Удаляем невалидные файлы
    for file in invalid_files:
        file_path = os.path.join(DATA_FOLDER, file)
        try:
            os.remove(file_path)
            context.log.info(f"Удалён невалидный файл: {file_path}")
        except Exception as e:
            context.log.error(f"Не удалось удалить файл {file_path}: {e}")

    # Если нет валидных файлов, завершаем
    if not valid_files:
        context.log.info("Нет валидных файлов для запуска обновления.")
        yield SkipReason("Нет валидных файлов.")
        return

    # Проверяем статус ранних запусков и создаём новые RunRequests
    for file in valid_files:
        file_path = os.path.join(DATA_FOLDER, file)

        # Если файл уже есть в состоянии, проверим статус раннего запуска
        if file in sensor_state:
            run_id = sensor_state[file]
            run = context.instance.get_run_by_id(run_id)

            if not run:
                # Если по какой-то причине run_id не найден, убираем файл из состояния и даём новый запуск
                context.log.warning(f"Run с id={run_id} не найден. Создаём новый запуск для файла {file}")
                del sensor_state[file]

            else:
                run_status = run.status

                # Проверяем статус: если ещё идёт (или в очереди), пропускаем
                if run_status in (
                    RunStatus.NOT_STARTED,
                    RunStatus.STARTING,
                    RunStatus.QUEUED,
                    RunStatus.MANAGED,
                    RunStatus.STARTED,
                    RunStatus.CANCELING
                ):
                    context.log.info(f"Файл {file} уже обрабатывается в run_id={run_id}, статус={run_status}. Пропускаем.")
                    continue

                # Если успешно завершился, удаляем файл и очищаем состояние
                if run_status == RunStatus.SUCCESS:
                    try:
                        os.remove(file_path)
                        context.log.info(f"Файл {file} успешно обработан и удалён.")
                    except Exception as e:
                        context.log.error(f"Не удалось удалить файл {file_path}: {e}")
                    del sensor_state[file]
                    continue

                # Если завершился с ошибкой, удаляем запись и запустим новый Run
                if run_status in (RunStatus.FAILURE, RunStatus.CANCELED):
                    context.log.warning(f"Файл {file} завершился ошибкой (run_id={run_id}, статус={run_status}). Перезапускаем.")
                    del sensor_state[file]

        # Если файла нет в состоянии или мы его удалили выше, создаём новый запуск
        if file not in sensor_state:
            context.log.info(f"Запуск процесса обновления для файла: {file}")

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

            # Создаём RunRequest без run_key (чтобы Dagster не блокировал повторные запуски)
            run_request = RunRequest(run_config=run_config)
            yield run_request

    # Сохраняем обновлённое состояние
    _save_state(context, sensor_state)
