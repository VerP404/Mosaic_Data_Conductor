from dagster import asset, Field, String, OpExecutionContext, AssetIn


@asset(
    config_schema={
        "mapping_file": Field(String, default_value="etl_wo/config/mapping.json"),
        "data_folder": Field(String, default_value="etl_wo/data/talon"),
        "table_key": Field(String, default_value="load_data_talons"),
    },
    ins={"db_check": AssetIn()}
)
def talon_extract2(context: OpExecutionContext, db_check: dict) -> dict:
    """
    Извлекает CSV-файл для таблицы 'load_data_talons'.
    Перед выполнением происходит проверка БД (результат передаётся через db_check).
    Все параметры можно переопределить через интерфейс Dagster.
    """

    config = context.op_config
    mapping_file = config["mapping_file"]
    data_folder = config["data_folder"]
    table_key = config["table_key"]

    from etl_wo.common.universal_extract import universal_extract
    result = universal_extract(context, mapping_file, data_folder, table_key)
    return result
