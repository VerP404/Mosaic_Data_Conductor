import dagster as dg
from etl_wo.jobs.kvazar import job_eln, kvazar_assets, job_emd, job_recipes, job_death, job_reference
from etl_wo.jobs.kvazar.sensor import kvazar_folder_monitor_sensor
from etl_wo.jobs.job1 import job_talons, talons_assets

all_assets = talons_assets + kvazar_assets
all_sensors = [
    kvazar_folder_monitor_sensor
]
defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons, job_eln, job_emd, job_recipes, job_death, job_reference],
    schedules=[],
    sensors=all_sensors
)
