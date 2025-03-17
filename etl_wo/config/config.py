import os
from dotenv import load_dotenv

load_dotenv()


def str_to_bool(s):
    return str(s).lower() in ("true", "1", "yes")


config = {
    "organizations": {
        "default": {
            "dbname": os.environ.get("ORG_DBNAME"),
            "user": os.environ.get("ORG_USER"),
            "password": os.environ.get("ORG_PASSWORD"),
            "host": os.environ.get("ORG_HOST"),
            "port": int(os.environ.get("ORG_PORT", 5432)),
            "tables": os.environ.get("ORG_TABLES", "load_data_talons").split(","),
            "selenium": {
                "enabled": str_to_bool(os.environ.get("ORG_SELENIUM_ENABLED", "true")),
                "download_mode": os.environ.get("ORG_SELENIUM_DOWNLOAD_MODE", "auto"),
                "browser": os.environ.get("ORG_SELENIUM_BROWSER", "chrome"),
                "oms_username": os.environ.get("ORG_SELENIUM_OMS_USERNAME"),
                "oms_password": os.environ.get("ORG_SELENIUM_OMS_PASSWORD"),
                "pzl_username": os.environ.get("ORG_SELENIUM_PZL_USERNAME"),
                "pzl_password": os.environ.get("ORG_SELENIUM_PZL_PASSWORD"),
            },

        }
    }
}

DATABASES = {
    'default': {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': os.environ.get('DB_PORT'),
    }
}

ORGANIZATIONS = os.environ.get('ORGANIZATIONS', 'МозаикаМед')
