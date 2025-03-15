from dagster import asset, OpExecutionContext, Field, StringSource, AssetIn
from etl_wo.common.universal_load import load_dataframe


def eln_sql_generator(data, table_name):
    # Исключаем автоматически генерируемые столбцы
    cols = [col for col in data.columns if col.lower() not in ("created_at", "updated_at")]
    # Формируем список столбцов для вставки: добавляем created_at и updated_at
    insert_columns = cols + ["created_at", "updated_at"]
    for _, row in data.iterrows():
        sql = f"""
        INSERT INTO {table_name} ({', '.join(insert_columns)})
        VALUES ({', '.join(['%s'] * len(cols))}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (number)
        DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in cols if col != 'number'])};
        """
        yield sql, tuple(row[col] for col in cols)


@asset(
    config_schema={
        "table_name": Field(StringSource, default_value="load_data_sick_leave_sheets", is_required=False)
    },
    ins={"eln_transform": AssetIn()}
)
def eln_load(context: OpExecutionContext, eln_transform: dict):
    """
    Загружает данные для таблицы load_data_sick_leave_sheets.
    Если данные отсутствуют, загрузка пропускается.
    """
    table_name = context.op_config.get("table_name", "load_data_sick_leave_sheets")
    data = eln_transform.get("data")

    if data is None or data.empty:
        context.log.info(f"ℹ️ Нет данных для загрузки в таблицу {table_name}.")
        return {"table_name": table_name, "status": "skipped"}

    return load_dataframe(context, table_name, data, db_alias="default", sql_generator=eln_sql_generator)
