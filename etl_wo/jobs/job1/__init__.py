from dagster import job, op
from .db_check import db_check
from .extract import talon_extract2


@job(name="job_talons")
def job_talons():
    db_result = db_check()
    talon_extract2(db_result)
