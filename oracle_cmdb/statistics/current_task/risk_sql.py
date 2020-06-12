# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaRiskSQL"
]

from collections import defaultdict
from typing import Union, Generator, Dict

from mongoengine import StringField, ListField, IntField, DictField

from utils.perf_utils import timing
from ..base import *
from .base import *
from rule.rule_jar import *
from ...capture import OracleSQLStatToday
from ...issue import OracleOnlineSQLIssue


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRiskSQL(OracleBaseCurrentTaskSchemaStatistics):
    """schema存在风险的SQL"""

    # 注意，风险SQL根据sql_id进行去重
    sql_id = StringField(null=True)
    level = IntField()
    rule_name = StringField()
    rule_desc = StringField(default="")
    rule_solution = ListField(default=list)
    issue_num = IntField(default=0)
    sql_stat = DictField(default=lambda: {
        "elapsed_time_total": None,
        "elapsed_time_delta": None,
        "executions_total": None,
        "executions_delta": None
    })

    meta = {
        "collection": "oracle_stats_schema_risk_sql",
        "indexes": [
            "sql_id",
            "level",
            "rule_name"
        ]
    }

    @classmethod
    @timing()
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRiskSQL", None, None]:
        issue_q = OracleOnlineSQLIssue.filter(
            task_record_id=task_record_id
        )
        cls.generate.tik(issue_q.count())
        rule_jar = RuleJar()
        sqls: Dict[str, cls] = defaultdict(cls)
        for an_issue in issue_q:
            sql_id = an_issue.output_params.sql_id
            doc = sqls[sql_id]
            doc.sql_id = sql_id
            if not doc.schema_name:
                # 优化，减少获取规则，填充规则的次数
                the_rule = rule_jar.get_rule(
                    cmdb_id=an_issue.cmdb_id, name=an_issue.rule_name)
                doc.schema_name = an_issue.schema_name
                doc.level = an_issue.level
                doc.rule_name = the_rule.name
                doc.rule_desc = the_rule.desc
                doc.rule_solution = the_rule.solution
            doc.issue_num += 1
            if not all(doc.sql_stat.values()):
                the_stat = OracleSQLStatToday.filter(
                    task_record_id=task_record_id,
                    sql_id=sql_id
                ).first()
                if the_stat:
                    doc.sql_stat = the_stat.to_dict(
                        iter_if=lambda k, v: k in doc.sql_stat.keys())
        cls.generate.tik(len(sqls))
        for doc in sqls.values():
            cls.post_generated(
                doc=doc,
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                schema_name=doc.schema_name
            )
            yield doc
