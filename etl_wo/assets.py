import dagster as dg
from etl_wo.jobs.eln import job_eln, eln_assets
from etl_wo.jobs.job1 import job_talons, talons_assets

all_assets = talons_assets + eln_assets

defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons, job_eln],
    schedules=[],  # Добавьте расписания, если они нужны
)
