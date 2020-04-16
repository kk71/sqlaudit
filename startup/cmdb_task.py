# Author: kk.Fang(fkfkbill@gmail.com)

from prettytable import PrettyTable

from models import init_models
# initiate database models/connections
init_models()
from models.sqlalchemy import *
from cmdb.cmdb_task import *


def main():
    """display all cmdb tasks"""
    with make_session() as session:
        task_q = session.query(
            *(qe := QueryEntity(
                CMDBTask.task_id,
                CMDBTask.task_type,
                CMDBTask.task_name,
                CMDBTask.cmdb_id,
                CMDBTask.connect_name,
                CMDBTask.group_name,
                CMDBTask.ip_address
            ))
        )
        pt = PrettyTable(qe.keys)
        pt.align = "l"
        for task in task_q:
            pt.add_row(task)
        print(pt)



