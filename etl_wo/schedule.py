from dagster import job, schedule
from etl_wo.etl.check_db import check_db
from etl_wo.etl.download_oms_file import download_oms_file
from etl_wo.etl.extract import extract
from etl_wo.etl.transform import transform
from etl_wo.etl.load_normal import load_normal
from etl_wo.etl.load_complex import load_complex


def make_oms_etl_job(org: str):
    @job(
        name=f"oms_etl_job_{org}",
        config={
            "ops": {
                "check_db": {"config": {"organization": org}},
                "download_oms_file": {"config": {"organization": org}},
                "load_normal": {"config": {"organization": org}},
                "load_complex": {"config": {"organization": org}}
            }
        }
    )
    def etl_job():
        db = check_db()
        file_path = download_oms_file()
        ext = extract(check_db=db, download_oms_file=file_path)
        trans = transform(extract=ext)
        load_normal(transform=trans)
        load_complex(transform=trans)

    return etl_job


oms_etl_job = make_oms_etl_job("default")


@schedule(
    cron_schedule="20 * * * *",
    job=oms_etl_job,
)
def daily_oms_schedule(context):
    return {}
