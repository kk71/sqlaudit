# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseReq",
    "OraclePrivilegeReq",
    "OraclePrettytableReq"
]

from prettytable import PrettyTable
from typing import Union, List

from restful_api.base import *
from auth.restful_api.base import *
from models.sqlalchemy import *
from ..cmdb import *
from ..auth.user_utils import *
from ..capture.sqlplan import OracleSQLPlanToday


class OracleBaseReq(BaseReq):

    def cmdbs(self, session) -> sqlalchemy_q:
        return session.query(OracleCMDB)

    def cmdb_ids(self, session) -> List[int]:
        return QueryEntity.to_plain_list(
            self.cmdbs(session).with_entities(OracleCMDB.cmdb_id))


class OraclePrivilegeReq(OracleBaseReq, PrivilegeReq):
    """登录用户的oracle相关接口"""

    def cmdbs(self, session) -> sqlalchemy_q:
        q = super().cmdbs(session)
        if not self.is_admin():
            # 如果是admin用户则可见任何纳管库
            cmdb_ids = current_cmdb(session, login_user=self.current_user)
            q = q.filter(OracleCMDB.cmdb_id.in_(cmdb_ids))
        return q

    def schemas(
            self,
            session,
            the_cmdb: Union[int, OracleCMDB]) -> List[str]:
        """当前登录用户在某个库的绑定schema"""
        if isinstance(the_cmdb, OracleCMDB):
            the_cmdb = the_cmdb.cmdb_id
        elif isinstance(the_cmdb, int):
            pass
        else:
            assert 0
        return current_schema(
            session,
            login_user=self.current_user,
            cmdb_id=the_cmdb
        )


class OraclePrettytableReq(OraclePrivilegeReq):

    def query_sqlplan(self, **params):

        plans_q = OracleSQLPlanToday.filter(**params).order_by("-create_time")
        latest_plan = plans_q.first()  # 取出最后一次采集出来的record_id
        task_record_id = latest_plan.task_record_id
        database_field = ["the_id", "operation_display", "options",
                          "object_name", "cardinality", "the_bytes", "cost", "time"]
        plans = plans_q.filter(task_record_id=task_record_id). \
            values_list(*database_field)
        return plans

    def output_table_sqlplan(self, plans):

        page_field = ["ID", "Operation", "Name",
                      "Rows", "Bytes", "Cost (%CPU)", "Time"]

        pt = PrettyTable(page_field)
        pt.align = "l"
        for plan in plans:
            plan = list(plan)
            m, s = divmod(plan[-1] if plan[-1] else 0, 60)
            h, m = divmod(m, 60)
            plan[-1] = "%02d:%02d:%02d" % (h, m, s)

            plan[1] = plan[1] + " " + plan[2] if plan[2] else plan[1]
            plan.pop(2)

            plan = [i if i is not None else " " for i in plan]

            if 8 > len(str(plan[-4])) > 5:
                plan[-4] = str(round(plan[-4] // 1024)) + "K"
                if len(str(plan[-4])) >= 8:
                    plan[-4] = str(round(plan[-4] // 1024 // 1024)) + "M"
            if 8 > len(str(plan[-3])) > 5:
                plan[-3] = str(round(plan[-3] // 1024)) + "K"
                if len(str(plan[-3])) >= 8:
                    plan[-3] = str(round(plan[-3] // 1024 // 1024)) + "M"
            if 8 > len(str(plan[-2])) > 5:
                plan[-2] = str(round(plan[-2] // 1024)) + "K"
                if len(str(plan[-2])) >= 8:
                    plan[-2] = str(round(plan[-2] // 1024 // 1024)) + "M"
            pt.add_row(plan)

        output_table_sqlplan = str(pt)
        return output_table_sqlplan

