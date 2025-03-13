from dagster import asset, Field, Array, String, OpExecutionContext
from etl_wo.common.check_db import check_db
from etl_wo.config.config import ORGANIZATIONS
from etl_wo.jobs.job1.flow_config import TABLE_NAME


@asset(
    config_schema={
        "organization": Field(String, default_value=ORGANIZATIONS),
        "tables": Field(Array(String), default_value=[TABLE_NAME])
    }
)
def db_check(context: OpExecutionContext) -> dict:
    """
    Проверяет подключение к базе и наличие указанных таблиц.
    Все сообщения выводятся через context.log.info() для единого лога.
    """
    config = context.op_config
    organization = config["organization"]
    tables = config["tables"]
    # Передаём context в функцию проверки
    result = check_db(context, organization=organization, db_alias='default', tables=tables)
    return result
