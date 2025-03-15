from dagster import job
from etl_wo.jobs.job1.db_check import talon_db_check
from .extract import talon_extract2
from .transform import talon_transform2
from .load_normal import talon_load_normal
from .load_complex import talon_load_complex

@job(name="job_talons")
def job_talons():
    db_result = talon_db_check()
    extract_result = talon_extract2(db_result)
    transform_result = talon_transform2(extract_result)
    normal_load = talon_load_normal(transform_result)
    complex_load = talon_load_complex(transform_result)
