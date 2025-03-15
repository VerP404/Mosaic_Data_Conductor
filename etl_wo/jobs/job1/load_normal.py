from dagster import asset, OpExecutionContext, Field, StringSource, AssetIn
from etl_wo.common.universal_load import load_dataframe

def normal_sql_generator(data, table_name):
    # Исключаем столбцы, которые не должны передаваться вручную: created_at и updated_at
    cols = [col for col in data.columns if col.lower() not in ("created_at", "updated_at")]
    # Формируем список столбцов для вставки: берем все наши данные и добавляем created_at и updated_at
    insert_columns = cols + ["created_at", "updated_at"]
    for _, row in data.iterrows():
        # Для обычных столбцов берем значения из row, а для created_at и updated_at вставляем CURRENT_TIMESTAMP прямо в запрос
        sql = f"""
        INSERT INTO {table_name} ({', '.join(insert_columns)})
        VALUES ({', '.join(['%s'] * len(cols))}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (talon, source)
        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in cols if col not in ('talon', 'source')])};
        """
        yield sql, tuple(row[col] for col in cols)

@asset(
    config_schema={
        "organization": Field(StringSource, default_value="default", is_required=False),
        "table_name": Field(StringSource, default_value="load_data_sick_leave_sheets", is_required=False)
    },
    ins={"talon_transform2": AssetIn()}
)
def talon_load_normal(context: OpExecutionContext, talon_transform2: dict):
    """
    Загружает данные для обычных талонов (режим upsert).
    """
    payload = talon_transform2.get("normal", {})
    table_name = payload.get("table_name")
    data = payload.get("data")

    if data is None or data.empty:
        context.log.info(f"ℹ️ Нет данных для таблицы {table_name} (обычные талоны).")
        return {"table_name": table_name, "status": "skipped"}

    return load_dataframe(context, table_name, data, db_alias="default", sql_generator=normal_sql_generator)
