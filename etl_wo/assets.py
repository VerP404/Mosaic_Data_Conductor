import dagster as dg
from etl_wo.jobs.kvazar import kvazar_job_eln, kvazar_assets, kvazar_job_emd, kvazar_job_recipes, kvazar_job_death, \
    kvazar_job_reference
from etl_wo.jobs.job1 import job_talons, talons_assets
from etl_wo.jobs.kvazar.sensor import kvazar_sensor_eln, kvazar_sensor_emd, kvazar_sensor_recipes, kvazar_sensor_death, \
    kvazar_sensor_reference

all_assets = talons_assets + kvazar_assets
all_sensors = [
    kvazar_sensor_eln,
    kvazar_sensor_emd,
    kvazar_sensor_recipes,
    kvazar_sensor_death,
    kvazar_sensor_reference
]
defs = dg.Definitions(
    assets=all_assets,
    jobs=[job_talons, kvazar_job_eln, kvazar_job_emd, kvazar_job_recipes, kvazar_job_death, kvazar_job_reference],
    schedules=[],
    sensors=all_sensors
)
