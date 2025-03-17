from dagster import job

from .db_check import kvazar_db_check
from .extract import kvazar_extract
from .load import kvazar_load
from .transform import kvazar_transform
from ...config.config import ORGANIZATIONS

kvazar_assets = [
    kvazar_db_check,
    kvazar_extract,
    kvazar_transform,
    kvazar_load
]


@job(
    name="job_eln",
    config={
        "ops": {
            "kvazar_db_check": {
                "config": {
                    "organization": ORGANIZATIONS,
                    "tables": ["load_data_sick_leave_sheets"]
                }
            },
            "kvazar_extract": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "data_folder": "etl_wo/data/eln",
                    "table_name": "load_data_sick_leave_sheets"
                }
            },
            "kvazar_transform": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "table_name": "load_data_sick_leave_sheets"
                }
            },
            "kvazar_load": {
                "config": {
                    "table_name": "load_data_sick_leave_sheets",
                    "data_folder": "etl_wo/data/eln",
                    "mapping_file": "etl_wo/config/mapping.json"
                }
            }
        }
    }
)
def job_eln():
    db_result = kvazar_db_check()
    extract_result = kvazar_extract(db_result)
    transform_result = kvazar_transform(extract_result)
    kvazar_load(transform_result)


@job(
    name="job_emd",
    config={
        "ops": {
            "kvazar_db_check": {
                "config": {
                    "organization": ORGANIZATIONS,
                    "tables": ["load_data_emd"]
                }
            },
            "kvazar_extract": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "data_folder": "etl_wo/data/emd",
                    "table_name": "load_data_emd"
                }
            },
            "kvazar_transform": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "table_name": "load_data_emd"
                }
            },
            "kvazar_load": {
                "config": {
                    "table_name": "load_data_emd",
                    "data_folder": "etl_wo/data/emd",
                    "mapping_file": "etl_wo/config/mapping.json"
                }
            }
        }
    }
)
def job_emd():
    db_result = kvazar_db_check()
    extract_result = kvazar_extract(db_result)
    transform_result = kvazar_transform(extract_result)
    kvazar_load(transform_result)


@job(
    name="job_recipes",
    config={
        "ops": {
            "kvazar_db_check": {
                "config": {
                    "organization": ORGANIZATIONS,
                    "tables": ["load_data_recipes"]
                }
            },
            "kvazar_extract": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "data_folder": "etl_wo/data/recipe",
                    "table_name": "load_data_recipes"
                }
            },
            "kvazar_transform": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "table_name": "load_data_recipes"
                }
            },
            "kvazar_load": {
                "config": {
                    "table_name": "load_data_recipes",
                    "data_folder": "etl_wo/data/recipe",
                    "mapping_file": "etl_wo/config/mapping.json"
                }
            }
        }
    }
)
def job_recipes():
    db_result = kvazar_db_check()
    extract_result = kvazar_extract(db_result)
    transform_result = kvazar_transform(extract_result)
    kvazar_load(transform_result)


@job(
    name="job_death",
    config={
        "ops": {
            "kvazar_db_check": {
                "config": {
                    "organization": ORGANIZATIONS,
                    "tables": ["load_data_death"]
                }
            },
            "kvazar_extract": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "data_folder": "etl_wo/data/death",
                    "table_name": "load_data_death"
                }
            },
            "kvazar_transform": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "table_name": "load_data_death"
                }
            },
            "kvazar_load": {
                "config": {
                    "table_name": "load_data_death",
                    "data_folder": "etl_wo/data/death",
                    "mapping_file": "etl_wo/config/mapping.json"
                }
            }
        }
    }
)
def job_death():
    db_result = kvazar_db_check()
    extract_result = kvazar_extract(db_result)
    transform_result = kvazar_transform(extract_result)
    kvazar_load(transform_result)


@job(
    name="job_reference",
    config={
        "ops": {
            "kvazar_db_check": {
                "config": {
                    "organization": ORGANIZATIONS,
                    "tables": ["load_data_reference"]
                }
            },
            "kvazar_extract": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "data_folder": "etl_wo/data/reference",
                    "table_name": "load_data_reference"
                }
            },
            "kvazar_transform": {
                "config": {
                    "mapping_file": "etl_wo/config/mapping.json",
                    "table_name": "load_data_reference"
                }
            },
            "kvazar_load": {
                "config": {
                    "table_name": "load_data_reference",
                    "data_folder": "etl_wo/data/reference",
                    "mapping_file": "etl_wo/config/mapping.json"
                }
            }
        }
    }
)
def job_reference():
    db_result = kvazar_db_check()
    extract_result = kvazar_extract(db_result)
    transform_result = kvazar_transform(extract_result)
    kvazar_load(transform_result)
