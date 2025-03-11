from dagster import job, schedule
from etl_wo.etl.check_db import check_db

from etl_wo.etl.talon.download_oms_file import talon_download_oms_file
from etl_wo.etl.talon.extract import talon_extract
from etl_wo.etl.talon.transform import talon_transform
from etl_wo.etl.talon.load_normal import talon_load_normal
from etl_wo.etl.talon.load_complex import talon_load_complex

from etl_wo.etl.eln.extract import sick_leave_extract
from etl_wo.etl.eln.transform import sick_leave_transform
from etl_wo.etl.eln.load import sink_leave_load


# Обработчик для талонов ОМС
@job(name="oms_etl_job")
def oms_etl_job():
    db = check_db()
    file_path = talon_download_oms_file()
    ext = talon_extract(check_db=db, talon_download_oms_file=file_path)
    trans = talon_transform(talon_extract=ext)
    talon_load_normal(talon_transform=trans)
    talon_load_complex(talon_transform=trans)


@schedule(
    cron_schedule="20 * * * *",
    job=oms_etl_job,
)
def daily_oms_schedule(context):
    return {}


# Обработчик для листов нетрудоспособности
@job(name="sick_leave_job")
def sick_leave_job():
    ext = sick_leave_extract()
    trans = sick_leave_transform(sick_leave_extract=ext)
    sink_leave_load(sick_leave_transform=trans)


@schedule(
    cron_schedule="0 0 * * *",  # Каждый день в 00:00
    job=sick_leave_job,
)
def daily_sick_leave_schedule(context):
    return {}
