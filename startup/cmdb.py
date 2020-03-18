# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()


def main():
    """display all cmdb info"""
    from models.oracle import make_session, TaskManage, QueryEntity
    from utils.const import DB_TASK_CAPTURE
    from prettytable import PrettyTable
    with make_session() as session:
        qe = QueryEntity(
            TaskManage.cmdb_id,
            TaskManage.task_id,
            TaskManage.connect_name,
            TaskManage.group_name,
            TaskManage.ip_address
        )
        task_q = session.query(*qe).filter(
            TaskManage.task_exec_scripts == DB_TASK_CAPTURE)
        pt = PrettyTable(qe.keys)
        pt.align = "l"
        for task in task_q:
            pt.add_row(task)
        print(pt)



