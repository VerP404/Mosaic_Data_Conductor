import glob
import os
import json
from dagster import asset, OpExecutionContext, Field, StringSource, AssetIn, String
from etl_wo.common.universal_load import load_dataframe

def clear_data_folder(data_folder):
    # Получаем список всех файлов в папке
    files = glob.glob(os.path.join(data_folder, '*'))
    for file in files:
        try:
            os.remove(file)
            print(f"Удалён файл: {file}")
        except Exception as e:
            print(f"Не удалось удалить {file}: {e}")

def kvazar_sql_generator(data, table_name, mapping_file):
    # Загружаем настройки маппинга из mapping_file
    if not os.path.exists(mapping_file):
        raise FileNotFoundError(f"Mapping file {mapping_file} not found.")
    with open(mapping_file, "r", encoding="utf-8") as f:
        mappings = json.load(f)
    table_config = mappings.get("tables", {}).get(table_name, {})
    conflict_columns = table_config.get("column_check", [])
    if not conflict_columns:
        raise ValueError(f"Conflict columns (column_check) not specified for table {table_name}.")
    conflict_columns_str = ", ".join(conflict_columns)

    # Исключаем автоматически генерируемые столбцы
    cols = [col for col in data.columns if col.lower() not in ("created_at", "updated_at")]
    # Формируем список столбцов для вставки: добавляем created_at и updated_at
    insert_columns = cols + ["created_at", "updated_at"]

    # Генерируем SQL-запросы с использованием конфликтных столбцов из маппинга
    for _, row in data.iterrows():
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in cols if col not in conflict_columns])
        sql = f"""
        INSERT INTO {table_name} ({', '.join(insert_columns)})
        VALUES ({', '.join(['%s'] * len(cols))}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT ({conflict_columns_str})
        DO UPDATE SET {update_clause};
        """
        yield sql, tuple(row[col] for col in cols)

@asset(
    config_schema={
        "table_name": Field(StringSource, is_required=True, description="Имя таблицы для загрузки"),
        "data_folder": Field(String, is_required=True, description="Путь к папке с CSV-файлами"),
        "mapping_file": Field(String, is_required=True, description="Путь к файлу mapping.json")
    },
    ins={"kvazar_transform": AssetIn()}
)
def kvazar_load(context: OpExecutionContext, kvazar_transform: dict):
    """
    Загружает данные для указанной таблицы.
    Все параметры (table_name, data_folder, mapping_file) передаются через op config.
    При формировании SQL используется список конфликтных столбцов из mapping.json (ключ column_check).
    """
    table_name = context.op_config["table_name"]
    data_folder = context.op_config["data_folder"]
    mapping_file = context.op_config["mapping_file"]
    data = kvazar_transform.get("data")

    if data is None or data.empty:
        context.log.info(f"ℹ️ Нет данных для загрузки в таблицу {table_name}.")
        return {"table_name": table_name, "status": "skipped"}

    # Вызываем универсальную функцию загрузки, передавая наш генератор SQL с mapping_file
    result = load_dataframe(
        context,
        table_name,
        data,
        db_alias="default",
        mapping_file=mapping_file,
        sql_generator=lambda d, t: kvazar_sql_generator(d, t, mapping_file)
    )

    if result.get("status") == "success":
        clear_data_folder(data_folder)
        context.log.info(f"✅ Папка {data_folder} очищена")

    return result
