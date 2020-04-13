# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from oracle_cmdb.tasks.cmdb_capture import *


def main():
    OracleCMDBCaptureTask.shoot(cmdb_task_id=1991, operator="123")
