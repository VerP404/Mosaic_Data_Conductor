import dagster as dg
from etl_wo.etl.check_db import check_db
from etl_wo.etl.download_oms_file import download_oms_file
from etl_wo.etl.extract import extract
from etl_wo.etl.transform import transform
from etl_wo.etl.load_normal import load_normal
from etl_wo.etl.load_complex import load_complex
from etl_wo.schedule import daily_oms_schedule

all_assets = [check_db, download_oms_file, extract, transform, load_normal, load_complex]

defs = dg.Definitions(
    assets=all_assets,
    schedules=[daily_oms_schedule],
)
