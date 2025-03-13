import dagster as dg
from etl_wo.jobs.job1.db_check import db_check
from etl_wo.jobs.job1.extract import talon_extract2
from etl_wo.jobs.job1.transform import talon_transform2
from etl_wo.jobs.job1 import job_talons, talon_load_complex, talon_load_normal

all_assets = [
    db_check,
    talon_extract2,
    talon_transform2,
    talon_load_complex,
    talon_load_normal
]

defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons],
    schedules=[],  # Добавьте расписания, если они нужны
)
