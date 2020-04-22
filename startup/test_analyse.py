# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from oracle_cmdb.issue.base import *
from models.sqlalchemy import *
from oracle_cmdb.cmdb import *
from cmdb.cmdb_task import *


def main():

    cmdb_task_id = 1991

    with make_session() as session:
        the_cmdb_task = session.query(CMDBTask).filter_by(
            id=cmdb_task_id).first()
        the_cmdb = session.query(OracleCMDB).filter_by(
            cmdb_id=the_cmdb_task.cmdb_id).first()
        cmdb_id = the_cmdb.cmdb_id
        schemas: [str] = the_cmdb.get_bound_schemas(session)
        print(f"{len(schemas)} schema(s) to run: {schemas}")

        OracleOnlineIssue.collect()
        OracleOnlineIssue.process(
            cmdb_id=cmdb_id,
            task_record_id=32,
            schemas=the_cmdb.get_bound_schemas(session)
        )
