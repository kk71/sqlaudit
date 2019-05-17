# configuration script for sqlaudit

# DO NOT CHANGE THIS FILE UNLESS YOU KNOW WHAT YOU ARE DOING
# almost all settings can be changed through environment variables
# please refer to docker-compose file and the py.env environment file first.
# to figure out the difference between default value and the current using value, please use app.py

from os import environ
from os import path


SETTINGS_FILE_DIR = path.dirname(__file__)
ALL_ENV_VARS = list()


def env_get(k, default, parser=None):
    final_value = environ.get(k, default)
    if parser:
        default = parser(default)
        final_value = parser(final_value)
    ALL_ENV_VARS.append((k, default, final_value, "!" if default != final_value else ""))
    return final_value


# ========= settings ==========

# web server settings

DEBUG = True
WEB_IP = env_get("WEB_IP", "localhost")  # TODO : DO NOT USE LOCALHOST
WEB_PORT = env_get("WEB_PORT", 8000)
JWT_ALGORITHM = env_get("JWT_ALGORITHM", "HS256")
JWT_SECRET = env_get("JWT_SECRET", "bZJc2sWbQLKos6GkHn/VB9oXwQt8S0R0kRvJ5/xJ89E1")
JWT_EXPIRE_SEC = env_get("JWT_EXPIRE_SEC", 3600, int)
UPLOAD_TO = env_get("UPLOAD_TO", "/tmp")
EXPORT_TO = path.join(SETTINGS_FILE_DIR, "downloads/export")
EXPORT_URL_WITHOUT_PREFIX = "/downloads/export"


# celery settings

REDIS_BROKER_IP = env_get("REDIS_BROKER_IP", WEB_IP)
REDIS_BROKER_PORT = env_get("REDIS_BROKER_PORT", 6379, int)
REDIS_BROKER_DB = env_get("REDIS_BROKER_DB", 2)
REDIS_BROKER = f'redis://:@{REDIS_BROKER_IP}:{REDIS_BROKER_PORT}/{REDIS_BROKER_DB}'

REDIS_BACKEND_IP = env_get("REDIS_BACKEND_IP", WEB_IP)
REDIS_BACKEND_PORT = env_get("REDIS_BACKEND_PORT", 6379, int)
REDIS_BACKEND_DB = env_get("REDIS_BACKEND_DB", 3)
REDIS_BACKEND = f'redis://:@{REDIS_BACKEND_IP}:{REDIS_BACKEND_PORT}/{REDIS_BACKEND_DB}'

from kombu import Exchange, Queue
CELERY_CONF = {
    # 'CELERYD_POOL_RESTARTS': True,
    'CELERY_TASK_SERIALIZER': 'json',
    'CELERY_ACCEPT_CONTENT': ['pickle', 'json', 'msgpack', 'yaml'],
    'CELERY_RESULT_SERIALIZER': 'json',
    'CELERYD_CONCURRENCY': 2,
    'CELERYD_MAX_TASKS_PER_CHILD': 5,
    'CELERY_ROUTES': {
        'task_exports.export': {'queue': 'task_exports', 'routing_key': 'task_exports'},
        'task_capture.task_run': {'queue': 'task_capture', 'routing_key': 'task_capture'},
        'task_sqlaitune.sqlaitune_run': {'queue': 'task_sqlaitune', 'routing_key': 'task_sqlaitune'},
        'task_mail.timing_send_email': {'queue': 'task_mail', 'routing_key': 'task_mail'},
        'task_submit_worklist.submit_worklist': {'queue': 'submit_worklist', 'routing_key': 'submit_worklist'},
    },
    'CELERY_QUEUE': {
        Queue('default', Exchange('default'), routing_key='default'),
        # Queue('task_exports', Exchange('task_exports'), routing_key='task_exports'),
        # Queue('task_capture', Exchange('task_capture'), routing_key='task_capture'),
        # Queue('task_sqlaitune', Exchange('task_sqlaitune'), routing_key='task_sqlaitune'),
        # Queue('task_mail', Exchange('task_mail'), routing_key='task_mail'),
        Queue('submit_ticket', Exchange('offline_ticket'), routing_key='submit_ticket'),
    },
    'CELERY_TIMEZONE': 'Asia/Shanghai',
}


# mongodb server settings

MONGO_SERVER = env_get("MONGO_IP", WEB_IP)
MONGO_PORT = env_get("MONGO_PORT", 27017, int)
MONGO_USER = env_get("MONGO_USER", "isqlaudit")
MONGO_PASSWORD = env_get("MONGO_PASSWORD", "v1g2m60id2499yz")
MONGO_DB = env_get("MONGO_DB", "sqlreview")


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
