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
RULE_DEBUG = env_get("RULE_DEBUG", 0, int)
API_DOC = env_get("API_DOC", 0, int)
URL_STATS = env_get("URL_STATS", 0, int)
WEB_IP = env_get("WEB_IP", "193.0.0.9")
WEB_PORT = env_get("WEB_PORT", 8000)
JWT_ALGORITHM = env_get("JWT_ALGORITHM", "HS256")
JWT_SECRET = env_get("JWT_SECRET", "bZJc2sWbQLKos6GkHn/VB9oXwQt8S0R0kRvJ5/xJ89E1")
JWT_EXPIRE_SEC = env_get("JWT_EXPIRE_SEC", 60 * 60 * 24, int)
UPLOAD_DIR = env_get("UPLOAD_DIR", "/tmp")
STATIC_DIR = path.join(SETTINGS_FILE_DIR, "downloads")
STATIC_PREFIX = "/downloads"
EXPORT_DIR = path.join(STATIC_DIR, "export")
HEALTH_DIR = path.join(STATIC_DIR, "health")
EXPORT_PREFIX = "/downloads/export"
EXPORT_PREFIX_HEALTH = "/downloads/health/"
TIMING_ENABLED = bool(env_get("TIMING_ENABLED", 0, int))
TIMING_THRESHOLD = env_get("TIMING_THRESHOLD", 0.5, float)
ADMIN_LOGIN_USER = "admin"
CLIENT_NAME = env_get("CLIENT_NAME", "Client Online Audit Report")  # change client name, this is for report mail
VERSION_FILE = path.join(SETTINGS_FILE_DIR, "version.json")
ECHO_SQL_WHEN_FAIL = env_get("ECHO_SQL_WHEN_FAIL", 1, int)


REDIS_IP_DEFAULT = env_get("REDIS_IP_DEFAULT", "193.0.0.3")

# celery broker & backend settings
# for celery other settings, please refer to celery_conf.py

REDIS_BROKER_IP = env_get("REDIS_BROKER_IP", REDIS_IP_DEFAULT)
REDIS_BROKER_PORT = env_get("REDIS_BROKER_PORT", 6379, int)
REDIS_BROKER_DB = env_get("REDIS_BROKER_DB", 2, int)
REDIS_BROKER = f'redis://{REDIS_BROKER_IP}:{REDIS_BROKER_PORT}/{REDIS_BROKER_DB}'

REDIS_BACKEND_IP = env_get("REDIS_BACKEND_IP", REDIS_IP_DEFAULT)
REDIS_BACKEND_PORT = env_get("REDIS_BACKEND_PORT", 6379, int)
REDIS_BACKEND_DB = env_get("REDIS_BACKEND_DB", 3)
REDIS_BACKEND = f'redis://{REDIS_BACKEND_IP}:{REDIS_BACKEND_PORT}/{REDIS_BACKEND_DB}'


# mongodb server settings

MONGO_SERVER = env_get("MONGO_IP", "193.0.0.2")
MONGO_PORT = env_get("MONGO_PORT", 27017, int)
MONGO_USER = env_get("MONGO_USER", "sqlreview")
MONGO_PASSWORD = env_get("MONGO_PASSWORD", "V1G2M60ID2499YZ")
MONGO_DB = env_get("MONGO_DB", "sqlreview")


# cache setting

CACHE_REDIS_IP = env_get("CACHE_REDIS_IP", REDIS_IP_DEFAULT)
CACHE_REDIS_PORT = env_get("CACHE_REDIS_PORT", REDIS_BACKEND_PORT, int)
CACHE_REDIS_DB = env_get("CACHE_REDIS_DB", 1, int)
CACHE_DEFAULT_EXPIRE_TIME = env_get("CACHE_DEFAULT_EXPIRE_TIME", 60*60*24, int)  # in sec


# license keys
PRIVATE_KEY = path.join(SETTINGS_FILE_DIR, "license/sqlaudit_license")
PUBLIC_KEY = path.join(SETTINGS_FILE_DIR, "license/sqlaudit_license.pub")


# logging

LOG_DEFAULT_PATH = "/tmp/"


# oracle connection settings

ORM_ECHO = env_get("ORM_ECHO", False, int)
MYSQL_IP = env_get("MYSQL_IP", "193.0.0.4")
MYSQL_PORT = env_get("MYSQL_PORT", 3306, int)
MYSQL_USERNAME = env_get("MYSQL_USERNAME", "isqlaudit")
MYSQL_PASSWORD = env_get("MYSQL_PASSWORD", "v1g2m60id2499yz")
MYSQL_DATABASE = env_get("MYSQL_DATABASE", "sqlaudit")


# the following is for plain sql connector for oracle

ORACLE_MIN_CONN = 1
ORACLE_MAX_CONN = 5
ORACLE_INCREMENT = 1


# max threads and max sub processes to run concurrent

CONC_MAX_THREAD = env_get("CONC_MAX_THREAD", 8, int)
CONC_MAX_PROCESS = env_get("CONC_MAX_PROCESS", 2, int)


# print out all referred environment variables with default value and the final value

from prettytable import PrettyTable
pt = PrettyTable(["environment variable", "default", "final", "different"])
for r in ALL_ENV_VARS:
    pt.add_row(r)
print(pt)
