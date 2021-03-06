# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "RuleAdapterSQL"
]

from typing import List, Optional

from .rule import BaseRule
from parsed_sql.single_sql import *
from cmdb.cmdb import CMDB


class RuleAdapterSQL:
    """
    一个专为针对SQL的规则写的适配器
    只为了保证运行规则的输入参数一致。即使是工单审核和线上审核。
    """

    def __init__(self, a_rule: BaseRule):
        self.a_rule = a_rule

    def run(self,
            entries: List[str],
            cmdb: Optional[CMDB] = None,
            single_sql: Optional[SingleSQL] = None,
            sqls: Optional[List[SingleSQL]] = None,
            cmdb_connector=None,
            sql_plan_qs=None,
            sql_stat_qs=None,
            schema_name: str = None,
            task_record_id: int = None,
            statement_id: str = None,
            snap_ids: (int, int) = None,
            ):
        """
        :param entries:
        :param cmdb:
        :param single_sql:
        :param sqls: 工单审核
        :param cmdb_connector:
        :param sql_plan_qs:
        :param sql_stat_qs: 线上审核
        :param schema_name:
        :param task_record_id: 线上审核
        :param statement_id: 工单审核
        :param snap_ids: 线上审核
        :return:
        """
        return self.a_rule.run(
            entries=entries,

            cmdb=cmdb,
            single_sql=single_sql,
            sqls=sqls,
            cmdb_connector=cmdb_connector,
            sql_plan_qs=sql_plan_qs,
            sql_stat_qs=sql_stat_qs,
            schema_name=schema_name,
            task_record_id=task_record_id,
            statement_id=statement_id,
            snap_ids=snap_ids
        )
