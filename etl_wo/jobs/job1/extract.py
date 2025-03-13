from dagster import asset, Field, String, OpExecutionContext, AssetIn

from etl_wo.jobs.job1.flow_config import TABLE_NAME, MAPPING_FILE, DATA_FOLDER


@asset(
    config_schema={
        "mapping_file": Field(String, default_value=MAPPING_FILE),
        "data_folder": Field(String, default_value=DATA_FOLDER),
        "table_name": Field(String, default_value=TABLE_NAME),
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
    table_name = config["table_name"]

    from etl_wo.common.universal_extract import universal_extract
    result = universal_extract(context, mapping_file, data_folder, table_name)
    return result
