from dagster import asset, Field, String, OpExecutionContext, AssetIn



@asset(
    config_schema={
        "mapping_file": Field(String),
        "data_folder": Field(String),
        "table_name": Field(String),
    },
    ins={"kvazar_db_check": AssetIn()}
)
def kvazar_extract(context: OpExecutionContext, kvazar_db_check: dict) -> dict:
    """
    Извлекает CSV-файл для таблицы.
    Перед выполнением происходит проверка БД (результат передаётся через db_check).
    Все параметры можно переопределить через интерфейс Dagster.
    """

    config = context.op_config
    mapping_file = config["mapping_file"]
    data_folder = config["data_folder"]
    table_name = config["table_name"]

    from etl_wo.common.universal_extract import universal_extract
    result = universal_extract(context, mapping_file, data_folder, table_name)
    return result
