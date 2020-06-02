# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaRiskObject"
]

from typing import Union, Generator

from mongoengine import StringField, ListField

from ..base import *
from .base import *
from oracle_cmdb.issue import OracleOnlineObjectIssue
from rule.rule_jar import *


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRiskObject(OracleBaseCurrentTaskSchemaStatistics):
    """schema的风险对象"""

    # TODO 注意，风险对象不会根据object_type-object_name进行去重

    rule_name = StringField(null=True)
    entries = ListField(default=list)  # 规则的entries
    object_name = StringField(null=True)
    object_type = StringField(null=True)
    issue_description = ListField(default=list)  # 规则输出数据的美化
    referred_table_name = StringField(null=True)  # 相关的表名
    # 因为表索引序列三大实体对象都是根据表名去展示的，这里需要记录一下对象相关的表名，以便展示

    meta = {
        "collection": "oracle_stats_schema_risk_object",
        "indexes": [
            "rule_name"
        ]
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRiskObject", None, None]:
        issue_q = OracleOnlineObjectIssue.filter(
            task_record_id=task_record_id
        )
        rule_jar = RuleJar()
        for an_issue in issue_q:
            the_rule = rule_jar.get_rule(
                cmdb_id=an_issue.cmdb_id, name=an_issue.rule_name)
            doc = cls(
                rule_name=the_rule.name,
                entries=the_rule.entries,
                issue_description=the_rule.format_output_params(an_issue.output_params),
                referred_table_name=an_issue.get_referred_table_name()
            )
            _, doc.object_type, doc.object_name = an_issue.get_object_unique_name()
            cls.post_generated(
                doc=doc,
                task_record_id=task_record_id,
                cmdb_id=cmdb_id,
                schema_name=an_issue.schema_name
            )
            yield doc
