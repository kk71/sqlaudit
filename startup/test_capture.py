# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from oracle_cmdb.tasks.cmdb_capture import *


def main():
    OracleCMDBCaptureTask.shoot(task_id=1991, cmdb_id=2526, operator="123")
