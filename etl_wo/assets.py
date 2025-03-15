import dagster as dg

from etl_wo.jobs.eln.db_check import eln_db_check
from etl_wo.jobs.eln.extract import eln_extract
from etl_wo.jobs.eln.transform import eln_transform
from etl_wo.jobs.eln.load import eln_load
from etl_wo.jobs.eln import job_eln

from etl_wo.jobs.job1.db_check import talon_db_check
from etl_wo.jobs.job1.extract import talon_extract2
from etl_wo.jobs.job1.transform import talon_transform2
from etl_wo.jobs.job1 import job_talons, talon_load_complex, talon_load_normal

all_assets = [
    talon_db_check,
    talon_extract2,
    talon_transform2,
    talon_load_complex,
    talon_load_normal,

    eln_db_check,
    eln_extract,
    eln_transform,
    eln_load
]

defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons, job_eln],
    schedules=[],  # Добавьте расписания, если они нужны
)
