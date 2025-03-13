import dagster as dg

# Импортируем asset-ы из вашего потока (job1)
from etl_wo.jobs.job1.db_check import db_check
from etl_wo.jobs.job1.extract import talon_extract2
# Импортируем job, определённую вручную в __init__.py
from etl_wo.jobs.job1 import job_talons

# Собираем все assets в список
all_assets = [
    db_check,
    talon_extract2,
]

# Регистрируем assets и job в Definitions
defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons],
    schedules=[],  # Добавьте расписания, если они нужны
)
