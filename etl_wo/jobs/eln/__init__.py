from dagster import job

from .db_check import eln_db_check
from .extract import eln_extract
from .load import eln_load
from .transform import eln_transform


@job(name="job_eln")
def job_eln():
    db_result = eln_db_check()
    extract_result = eln_extract(db_result)
    transform_result = eln_transform(extract_result)
    load = eln_load(transform_result)