from dagster import asset, Field, Array, String, OpExecutionContext
from etl_wo.common.check_db import check_db
from etl_wo.config.config import ORGANIZATIONS


@asset(
    config_schema={
        "organization": Field(String, default_value=ORGANIZATIONS),
        "tables": Field(Array(String))
    }
)
def kvazar_db_check(context: OpExecutionContext) -> dict:
    """
    Проверяет подключение к базе и наличие указанных таблиц.
    Все сообщения выводятся через context.log.info() для единого лога.
    """
    config = context.op_config
    organization = config["organization"]
    tables = config["tables"]
    # Передаём context в функцию проверки
    result = check_db(context, organization=organization, tables=tables)
    return result
