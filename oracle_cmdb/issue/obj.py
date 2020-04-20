# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssue"
]

from typing import Generator

import rule.const
from models.sqlalchemy import *
from rule.rule import CMDBRule
from .base import *
from utils.log_utils import *
from ..cmdb import *


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssue(OracleOnlineIssue):
    """oracle线上审核对象问题"""

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_OBJECT,)

    @classmethod
    def simple_analyse(cls, **kwargs) -> Generator["OracleOnlineObjectIssue"]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        with make_session() as session:
            cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            rule_jar: [CMDBRule] = cls.generate_rule_jar()
            entries = cls.inherited_entries()
            cmdb_connector = cmdb.build_connector()
            with grouped_count_logger(
                    cls.__doc__, item_type_name="rule") as counter:
                for the_rule in rule_jar:
                    docs = []
                    ret = the_rule.run(
                        entries=entries,

                        cmdb=cmdb,
                        task_record_id=task_record_id,
                        cmdb_connector=cmdb_connector,
                        schema_name=schema_name
                    )
                    for minus_score, output_param in ret:
                        doc = cls()
                        doc.as_issue_of(
                            the_rule,
                            output_data=output_param,
                            minus_score=minus_score
                        )
                        docs.append(doc)
                        yield doc
                    counter(the_rule.name, len(docs))
