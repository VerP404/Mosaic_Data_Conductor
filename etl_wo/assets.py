import dagster as dg
from etl_wo.etl.check_db import check_db
from etl_wo.etl.eln.extract import sick_leave_extract
from etl_wo.etl.eln.transform import sick_leave_transform
from etl_wo.etl.eln.load import sink_leave_load
from etl_wo.etl.talon.download_oms_file import talon_download_oms_file
from etl_wo.etl.talon.extract import talon_extract
from etl_wo.etl.talon.transform import talon_transform
from etl_wo.etl.talon.load_normal import talon_load_normal
from etl_wo.etl.talon.load_complex import talon_load_complex
from etl_wo.etl.people.download_people_iszl import download_pzl_people_file
from etl_wo.schedule import daily_oms_schedule, daily_sick_leave_schedule

all_assets = [download_pzl_people_file,
              check_db,
              talon_download_oms_file,
              talon_extract,
              talon_transform,
              talon_load_normal,
              talon_load_complex,

              sick_leave_extract,
              sick_leave_transform,
              sink_leave_load


              ]

defs = dg.Definitions(
    assets=all_assets,
    schedules=[daily_oms_schedule, daily_sick_leave_schedule],
)
