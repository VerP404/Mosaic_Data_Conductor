from dagster import asset, OpExecutionContext, AssetIn
import numpy as np
from etl_wo.common.universal_load import load_dataframe

def complex_sql_generator(data, table_name):
    # Исключаем столбцы, которые генерируются автоматически: created_at и updated_at
    cols = [col for col in data.columns if col.lower() not in ("created_at", "updated_at")]
    # Формируем итоговый список столбцов для INSERT: обычные столбцы + created_at и updated_at
    insert_columns = cols + ["created_at", "updated_at"]
    groups = data.groupby(["talon", "source"])
    for (talon, source), group in groups:
        talon_key = str(talon) if isinstance(talon, np.generic) else talon
        source_key = str(source) if isinstance(source, np.generic) else source
        # Удаляем существующие записи по ключу
        delete_sql = f"DELETE FROM {table_name} WHERE talon = %s AND source = %s;"
        yield delete_sql, (talon_key, source_key)
        # Для каждой группы формируем запрос на вставку, где для created_at и updated_at вставляем CURRENT_TIMESTAMP
        for _, row in group.iterrows():
            sql = f"""
            INSERT INTO {table_name} ({', '.join(insert_columns)})
            VALUES ({', '.join(['%s'] * len(cols))}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
            """
            values = tuple(row[col] for col in cols)
            yield sql, values

@asset(
    ins={"talon_transform2": AssetIn()}
)
def talon_load_complex(context: OpExecutionContext, talon_transform2: dict):
    """
    Загружает данные для комплексных талонов.
    """
    payload = talon_transform2.get("complex", {})
    table_name = payload.get("table_name")
    data = payload.get("data")

    if data is None or data.empty:
        context.log.info(f"ℹ️ Нет данных для таблицы {table_name} (комплексные талоны).")
        return {"table_name": table_name, "status": "skipped"}

    return load_dataframe(context, table_name, data, db_alias="default", sql_generator=complex_sql_generator)
