import json
import psycopg2
from sqlalchemy import create_engine
from dagster import asset, Output, OpExecutionContext
from etl_wo.config.config import config

# Загружаем настройки подключения к БД
organizations = config.get("organizations", {})

@asset
def check_db(context: OpExecutionContext):
    """
    Проверяет подключение к базе данных и наличие таблицы в .env.
    """
    # Получаем название организации из конфигурации (по умолчанию "local")
    org = context.op_config.get("organization", "local")
    if org not in organizations:
        text_value = f"❌ База {org} не найдена в .env"
        context.log.info(text_value)
        raise ValueError(text_value)

    db_config = organizations[org]

    # Подключаемся через SQLAlchemy
    try:
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        with engine.connect() as conn:
            conn.close()
        text_value = f"✅ Подключение к {org}.{db_config['dbname']} успешно!"
        context.log.info(text_value)
        print(text_value)
    except Exception as e:
        text_value = f"❌ Ошибка подключения к БД {org}: {e}"
        context.log.info(text_value)
        raise ValueError(text_value)

    # Проверяем таблицы
    tables = db_config.get("tables", [])
    if not tables:
        text_value = f"❌ В БД {org} нет зарегистрированных таблиц."
        context.log.info(text_value)
        raise ValueError(text_value)

    # Для каждой таблицы выводим количество строк
    with engine.connect() as conn:
        for table in tables:
            try:
                from sqlalchemy import text
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                context.log.info(f"📋 Таблица {table}: {result} строк")
            except Exception as ex:
                context.log.info(f"⚠️ Не удалось получить число строк для таблицы {table}: {ex}")

    text_value = f"📋 В БД {org} найдены таблицы: {', '.join(tables)}"
    context.log.info(text_value)
    return {"db_name": org, "tables": tables}
