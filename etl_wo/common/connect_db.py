import psycopg2
from sqlalchemy import create_engine
from etl_wo.config.config import DATABASES, ORGANIZATIONS


def connect_to_db(db_alias='default', organization=ORGANIZATIONS, context=None):
    """
    Подключается к базе данных, используя настройки из config/config.py.
    Если передан context, выводит сообщения через context.log.info().

    :param db_alias: Ключ конфигурации в DATABASES (по умолчанию 'default')
    :param organization: Название организации (можно задать через конфигурацию джобы)
    :param context: (Опционально) Dagster context для логирования
    :return: Кортеж (engine, psycopg2 connection)
    """
    if db_alias not in DATABASES:
        err_msg = f"❌ Конфигурация для базы '{db_alias}' в организации '{organization}' не найдена."
        if context:
            context.log.info(err_msg)
        else:
            print(err_msg)
        raise ValueError(err_msg)

    db_config = DATABASES[db_alias]
    connection_str = (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    engine = create_engine(connection_str)
    try:
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
    except Exception as e:
        err_msg = f"❌ Ошибка подключения к базе {db_config['dbname']} в организации '{organization}': {e}"
        if context:
            context.log.info(err_msg)
        else:
            print(err_msg)
        raise ValueError(err_msg)

    success_msg = f"✅ Подключение к базе {db_config['dbname']} в организации '{organization}' успешно!"
    if context:
        context.log.info(success_msg)
    else:
        print(success_msg)
    return engine, conn
