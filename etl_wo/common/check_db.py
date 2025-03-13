from sqlalchemy import text
from etl_wo.common.connect_db import connect_to_db
from etl_wo.config.config import DATABASES


def check_db(context, organization='default', db_alias='default', tables=None):
    """
    Проверяет подключение к базе данных и наличие заданных таблиц.
    Выводит сообщения через context.log.info().

    :param context: Dagster context для логирования
    :param organization: Название организации (можно задать через конфигурацию джобы)
    :param db_alias: Ключ конфигурации в DATABASES (по умолчанию 'default')
    :param tables: Список таблиц для проверки (обязательно должен быть передан)
    :return: Словарь с информацией о проверке
    """
    if not tables:
        message = "❌ Список таблиц для проверки не задан."
        context.log.info(message)
        raise ValueError(message)

    if db_alias not in DATABASES:
        message = f"❌ Конфигурация для базы '{db_alias}' не найдена."
        context.log.info(message)
        raise ValueError(message)

    db_config = DATABASES[db_alias]
    engine, conn = connect_to_db(db_alias, organization=organization, context=context)

    try:
        with engine.connect() as connection:
            for table in tables:
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    message = f"📋 Таблица '{table}': {result} строк"
                    context.log.info(message)
                except Exception as e:
                    message = f"⚠️ Не удалось получить количество строк для таблицы '{table}': {e}"
                    context.log.info(message)
    finally:
        conn.close()

    message = f"📋 Проверка завершена. Проверены таблицы: {', '.join(tables)}"
    context.log.info(message)
    return {"db_alias": db_alias, "organization": organization, "tables": tables}
