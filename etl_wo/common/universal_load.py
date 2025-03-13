# common/universal_load.py
import psycopg2
from dagster import OpExecutionContext

from etl_wo.common.connect_db import connect_to_db


def load_dataframe(context: OpExecutionContext, table_name: str, data, db_alias: str, sql_generator) -> dict:
    """
    Универсальная функция для загрузки DataFrame в таблицу БД.
    Использует уже настроенное подключение через connect_to_db.

    :param context: Dagster execution context.
    :param table_name: Имя таблицы в БД.
    :param data: Pandas DataFrame с данными для загрузки.
    :param db_alias: Ключ подключения (например, 'default').
    :param sql_generator: Функция, генерирующая SQL-запросы и параметры.
    :return: Словарь с итоговым числом строк, статусом и именем таблицы.
    """
    # Удаляем столбцы, которые генерируются автоматически (например, created_at и updated_at)
    data = data.drop(columns=[col for col in data.columns if col.lower() in ("created_at", "updated_at")],
                     errors='ignore')

    # Получаем подключение к базе (используется уже настроенная функция)
    engine, conn = connect_to_db(db_alias=db_alias, organization=None, context=context)
    cursor = conn.cursor()

    # Заполняем отсутствующие значения
    data.fillna("-", inplace=True)

    # Выполняем SQL-запросы, сгенерированные sql_generator'ом
    for sql, params in sql_generator(data, table_name):
        cursor.execute(sql, params)

    conn.commit()

    # Получаем итоговое число строк в таблице
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    final_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    context.log.info(f"📤 Данные загружены в {table_name}. Итоговое число строк: {final_count}")
    return {"table_name": table_name, "status": "success", "final_count": final_count}
