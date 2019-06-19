# configuration script for sqlaudit

# DO NOT CHANGE THIS FILE UNLESS YOU KNOW WHAT YOU ARE DOING
# almost all settings can be changed through environment variables
# please refer to docker-compose file and the py.env environment file first.

from os import environ
from os import path


SETTINGS_FILE_DIR = path.dirname(path.realpath(__file__))
ALL_ENV_VARS = list()


def env_get(k, default, parser=None):
    final_value = environ.get(k, default)
    if parser:
        default = parser(default)
        final_value = parser(final_value)
    ALL_ENV_VARS.append((k, default, final_value, "!" if default != final_value else ""))
    return final_value


# ==================  settings ===================

# web server settings

DEBUG = True
WEB_IP = env_get("WEB_IP", "localhost")  # TODO : DO NOT USE LOCALHOST
WEB_PORT = env_get("WEB_PORT", 8000)
JWT_ALGORITHM = env_get("JWT_ALGORITHM", "HS256")
JWT_SECRET = env_get("JWT_SECRET", "bZJc2sWbQLKos6GkHn/VB9oXwQt8S0R0kRvJ5/xJ89E1")
JWT_EXPIRE_SEC = env_get("JWT_EXPIRE_SEC", 3600000, int)
UPLOAD_DIR = env_get("UPLOAD_DIR", "/tmp")
STATIC_DIR = path.join(SETTINGS_FILE_DIR, "downloads")
STATIC_PREFIX = "/downloads"
EXPORT_DIR = path.join(STATIC_DIR, "export")
EXPORT_PREFIX = "/downloads/export"
TIMING_ENABLED = bool(env_get("TIMING_ENABLED", 1, int))
TIMING_THRESHOLD = env_get("TIMING_THRESHOLD", 0.3, float)


# celery broker & backend settings
# for celery other settings, please refer to celery_conf.py

REDIS_BROKER_IP = env_get("REDIS_BROKER_IP", WEB_IP)
REDIS_BROKER_PORT = env_get("REDIS_BROKER_PORT", 6379, int)
REDIS_BROKER_DB = env_get("REDIS_BROKER_DB", 2, int)
REDIS_BROKER = f'redis://{REDIS_BROKER_IP}:{REDIS_BROKER_PORT}/{REDIS_BROKER_DB}'

REDIS_BACKEND_IP = env_get("REDIS_BACKEND_IP", WEB_IP)
REDIS_BACKEND_PORT = env_get("REDIS_BACKEND_PORT", 6379, int)
REDIS_BACKEND_DB = env_get("REDIS_BACKEND_DB", 3)
REDIS_BACKEND = f'redis://{REDIS_BACKEND_IP}:{REDIS_BACKEND_PORT}/{REDIS_BACKEND_DB}'


# mongodb server settings

MONGO_SERVER = env_get("MONGO_IP", WEB_IP)
MONGO_PORT = env_get("MONGO_PORT", 27017, int)
MONGO_USER = env_get("MONGO_USER", "sqlreview")
MONGO_PASSWORD = env_get("MONGO_PASSWORD", "V1G2M60ID2499YZ")
MONGO_DB = env_get("MONGO_DB", "sqlreview")


# cache setting

CACHE_REDIS_SERVER = env_get("CACHE_REDIS_IP", WEB_IP)
CACHE_REDIS_PORT = env_get("CACHE_REDIS_PORT", 6379, int)
CACHE_REDIS_DB = env_get("CACHE_REDIS_DB", 1, int)
CACHE_DEFAULT_EXPIRE_TIME = env_get("CACHE_DEFAULT_EXPIRE_TIME", 60*60*24, int)  # in sec


# license keys

PRIVATE_KEY = "/project/sqlaudit_license"
PUBLIC_KEY = "/project/sqlaudit_license.pub"


# logging

LOG_DEFAULT_PATH = "/tmp/"


# oracle connection settings

ORM_ECHO = False
ORACLE_IP = env_get("ORACLE_IP", WEB_IP)
ORACLE_USERNAME = env_get("ORACLE_USERNAME", "isqlaudit")
ORACLE_PASSWORD = env_get("ORACLE_PASSWORD", "v1g2m60id2499yz")
ORACLE_PORT = env_get("ORACLE_PORT", "1521")
ORACLE_SID = env_get("ORACLE_SID", "sqlaudit")

# the following is for plain sql connector for oracle
ORACLE_MIN_CONN = 1
ORACLE_MAX_CONN = 5
ORACLE_INCREMENT = 1


# print out all referred environment variables with default value and the final value

from prettytable import PrettyTable
pt = PrettyTable(["environment variable", "default", "final", "different"])
for r in ALL_ENV_VARS:
    pt.add_row(r)
print(pt)
