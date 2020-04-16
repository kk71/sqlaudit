# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

import cmdb.const
from oracle_cmdb.tasks.cmdb_capture import *


@click.argument("task_id", type=click.INT, required=True)
def main(task_id):
    """manually start a capture"""
    if not task_id:
        print("task_id is required. ")
        print("If you don't know the task_id to some cmdb,"
              " use './app.sh cmdb-task'.")
        return
    OracleCMDBCaptureTask.shoot(
        cmdb_task_id=int(task_id),
        operator=cmdb.const.CMDB_TASK_OPERATOR_CLI
    )
