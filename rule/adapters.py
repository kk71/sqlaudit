# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBRuleAdapterSQL"
]

from .cmdb_rule import CMDBRule
from parsed_sql.single_sql import *
from cmdb.cmdb import CMDB


class CMDBRuleAdapterSQL:
    """
    一个专为针对SQL的规则写的适配器
    只为了保证运行规则的输入参数一致。即使是工单审核和线上审核。
    """

    def __init__(self, a_rule: CMDBRule):
        self.a_rule = a_rule

    def run(self,
            entries: [str],
            cmdb: CMDB,
            single_sql: SingleSQL = None,
            sqls: [SingleSQL] = None,
            cmdb_connector=None,
            sql_plan_qs=None,
            schema_name: str = None,
            task_record_id: int = None,
            statement_id: str = None,
            sql_id: str = None
            ):
        """
        :param entries:
        :param cmdb:
        :param single_sql:
        :param sqls: 工单审核
        :param cmdb_connector:
        :param sql_plan_qs: 工单动态审核
        :param schema_name:
        :param task_record_id: 线上审核
        :param statement_id: 工单审核
        :param sql_id: 线上审核
        :return:
        """
        return self.a_rule.run(
            entries=entries,

            cmdb=cmdb,
            single_sql=single_sql,
            sqls=sqls,
            cmdb_connector=cmdb_connector,
            sql_plan_qs=sql_plan_qs,
            schema_name=schema_name,
            task_record_id=task_record_id,
            statement_id=statement_id,
            sql_id=sql_id
        )
