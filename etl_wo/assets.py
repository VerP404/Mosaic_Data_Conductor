import dagster as dg
from etl_wo.jobs.eln import job_eln, eln_assets
from etl_wo.jobs.eln.sensor import eln_folder_monitor_sensor
from etl_wo.jobs.job1 import job_talons, talons_assets

all_assets = talons_assets + eln_assets
all_sensors = [
    eln_folder_monitor_sensor
]
defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons, job_eln],
    schedules=[],
    sensors=all_sensors
)
