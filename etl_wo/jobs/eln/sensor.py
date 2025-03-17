import os
import time
import json
import fnmatch

from dagster import (
    sensor,
    RunRequest,
    SkipReason,
    DagsterInstance,  # для get_runs
    RunStatus
)
from dagster._core.storage.pipeline_run import RunsFilter

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
    Сенсор для мониторинга папки DATA_FOLDER с учётом состояния:
      1. Если файл уже обрабатывается (запуск с run_key ещё идёт), повторно не запускаем.
      2. Если предыдущая обработка завершилась успехом – удаляем файл и чистим состояние.
      3. Если предыдущая обработка завершилась ошибкой – перезапускаем (создаём новый run_key).
    """

    sensor_state = _load_state(context)  # {filename: run_key}

    # 1. Проверяем наличие файла маппинга
    if not os.path.exists(MAPPING_FILE):
        context.log.info(f"❌ Файл маппинга {MAPPING_FILE} не найден.")
        yield SkipReason("Mapping file not found.")
        return

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

    # 2. Проверяем папку с данными
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

    # 3. Разделяем файлы на валидные и невалидные
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

    # 4. Удаляем невалидные файлы
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

    # 5. Для каждого валидного файла проверяем состояние
    for file in valid_files:
        file_path = os.path.join(DATA_FOLDER, file)

        if file in sensor_state:
            # Файл уже запускался, проверим, чем закончился запуск
            existing_run_key = sensor_state[file]
            runs = context.instance.get_runs(filters=RunsFilter(run_keys=[existing_run_key]))
            if not runs:
                # Не нашли run с таким ключом – возможно, что-то пошло не так, убираем ключ и даём новый запуск
                context.log.warning(f"Не нашли Run c run_key={existing_run_key}. Перезапускаем для файла {file}.")
                del sensor_state[file]
            else:
                run = runs[-1]  # обычно будет один, но возьмём последний
                status = run.status
                context.log.info(f"Файл={file}, run_key={existing_run_key}, status={status}.")

                if status in (RunStatus.NOT_STARTED, RunStatus.STARTING, RunStatus.QUEUED, RunStatus.STARTED, RunStatus.MANAGED, RunStatus.CANCELING):
                    # Запуск ещё идёт – пропускаем
                    continue

                if status == RunStatus.SUCCESS:
                    # Успешно – удаляем файл, очищаем состояние
                    try:
                        os.remove(file_path)
                        context.log.info(f"Файл {file} успешно обработан и удалён.")
                    except Exception as e:
                        context.log.error(f"Не удалось удалить файл {file_path}: {e}")
                    del sensor_state[file]
                    continue

                if status in (RunStatus.FAILURE, RunStatus.CANCELED):
                    # Ошибка – убираем запись и дадим новый запуск
                    context.log.warning(f"Файл {file} завершился ошибкой. Будет повторная попытка.")
                    del sensor_state[file]

        # Если мы дошли сюда, значит:
        # - файла нет в sensor_state
        # ИЛИ
        # - мы только что удалили его оттуда (ошибка или run не найден)
        if file not in sensor_state:
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

            yield RunRequest(
                run_key=new_run_key,
                run_config=run_config
            )
            # Запомним, что для этого файла был запуск
            sensor_state[file] = new_run_key

    # 6. Сохраняем обновлённое состояние
    _save_state(context, sensor_state)
