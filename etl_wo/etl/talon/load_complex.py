import json
import psycopg2
import numpy as np
from dagster import asset, OpExecutionContext, Field, StringSource

from etl_wo.config.config import config as env_config

organizations = env_config.get("organizations", {})


@asset(
    config_schema={
        "organization": Field(StringSource, default_value="local", is_required=False)
    }
)
def talon_load_complex(context: OpExecutionContext, talon_transform: dict):
    """Загружает данные для комплексных талонов в таблицу load_data_complex_talons."""
    payload = talon_transform.get("complex", {})
    table_name = payload.get("table_name")
    data = payload.get("data")

    org = context.op_config.get("organization", "local")
    if org not in organizations:
        raise ValueError(f"База {org} не найдена в db_connections.json")
    db_config = organizations[org]

    conn = psycopg2.connect(
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
        host=db_config["host"],
        port=db_config["port"]
    )
    cursor = conn.cursor()

    if data is None or data.empty:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        text_value = f"ℹ️ Нет данных для таблицы {table_name} (комплексные талоны). Итоговое число строк: {final_count}"
        context.log.info(text_value)
        print(text_value)
        return {"table_name": table_name, "status": "skipped"}

    data.fillna("-", inplace=True)
    groups = data.groupby(["talon", "source"])
    for (talon, source), group in groups:
        talon_key = str(talon) if isinstance(talon, np.generic) else talon
        source_key = str(source) if isinstance(source, np.generic) else source
        delete_sql = f"DELETE FROM {table_name} WHERE talon = %s AND source = %s;"
        cursor.execute(delete_sql, (talon_key, source_key))
        for _, row in group.iterrows():
            insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(data.columns)})
            VALUES ({', '.join(['%s'] * len(data.columns))});
            """
            values = tuple(x.item() if hasattr(x, "item") else x for x in row)
            cursor.execute(insert_sql, values)

    conn.commit()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    final_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    context.log.info(f"📤 Комплексные талоны загружены в {table_name}. Итоговое число строк: {final_count}")
    return {"table_name": table_name, "status": "success"}
