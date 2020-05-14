# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsSchemaRiskRule"
]

from typing import Union, Generator, List

from mongoengine import DictField, IntField, StringField, ListField

from ..base import *
from .base import *
from oracle_cmdb.issue import \
    OracleOnlineIssue, OracleOnlineObjectIssue, OracleOnlineSQLIssue
from rule.cmdb_rule import CMDBRule


@OracleBaseStatistics.need_collect()
class OracleStatsSchemaRiskRule(OracleBaseCurrentTaskSchemaStatistics):
    """schema存在风险的规则"""

    entry = StringField(required=True)
    entries = ListField(default=list)
    rule = DictField(default=lambda: {
        # 这里的数据来自规则仓库
        "name": "",
        "desc": "",
        "summary": "",
        "solution": []
                                    })
    level = IntField()
    issue_num = IntField(default=0)

    meta = {
        "collection": "oracle_stats_schema_risk_rule",
        "indexes": [
            "rule.name",
            "level",
            "issue_num"
        ]
    }

    ISSUES = (
        OracleOnlineObjectIssue,
        OracleOnlineSQLIssue
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsSchemaRiskRule", None, None]:
        schemas: List[str] = kwargs["schemas"]
        cmdb_rule_dict = dict()  # 放个缓存

        for schema_name in schemas:
            for issue_model in cls.ISSUES:
                ret = OracleOnlineIssue.aggregate(
                    {
                        "$match": {
                            "task_record_id": task_record_id,
                            "schema_name": schema_name
                        }
                    },
                    {
                        "$addFields": {
                            # 用于保留原本的entries上层结构
                            "original_entries": issue_model.INHERITED_ENTRIES
                        }
                    },
                    {
                        "$unwind": {
                            "path": "$entries"
                        }
                    },
                    {
                        "$match": {
                            # 注意这里的entries已经不是list了
                            "entries": {
                                "$in": issue_model.ENTRIES
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "rule_name": "$rule_name",
                                "level": "$level",
                                "entry": "$entries",
                                "entries": "$original_entries"
                            },
                            "issue_num": {"$sum": 1}
                        }
                    }
                )
                for i in ret:
                    doc = cls(
                        entries=i["_id"]["entries"]
                    )
                    doc.entry = i["_id"]["entry"]
                    doc.level = i["_id"]["level"]
                    doc.issue_num = i["issue_num"]
                    rule_name = i["_id"]["rule_name"]
                    the_rule_dict = cmdb_rule_dict.get(rule_name, None)
                    if not the_rule_dict:
                        the_rule = CMDBRule.filter(
                            cmdb_id=cmdb_id,
                            name=rule_name
                        ).first()
                        if not the_rule:
                            print(f"{rule_name} should exists but {the_rule=}, ignored.")
                            continue
                        the_rule_dict = cmdb_rule_dict[rule_name] = the_rule.to_dict(
                            iter_if=lambda k, v: k in doc.rule.keys())
                    doc.rule = the_rule_dict

                    cls.post_generated(
                        doc=doc,
                        task_record_id=task_record_id,
                        cmdb_id=cmdb_id,
                        schema_name=schema_name
                    )
                    yield doc

    @classmethod
    def filter(cls, *args, **kwargs):
        return super().filter(*args, **kwargs).order_by(
            "-level", "-issue_num")
