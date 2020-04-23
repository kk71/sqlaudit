# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssue"
]

from typing import Generator

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from models.sqlalchemy import *
from rule.cmdb_rule import CMDBRule
from issue.issue import OnlineIssueOutputParams
from .base import *
from ..cmdb import *


class OracleOnlineIssueOutputParamsObject(OnlineIssueOutputParams):
    """针对对象的输出字段"""

    object_name = StringField(required=True, default=None)
    object_type = StringField(required=True, default=None)


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssue(OracleOnlineIssue):
    """对象问题"""

    output_params = EmbeddedDocumentField(
        OracleOnlineIssueOutputParamsObject, default=OracleOnlineIssueOutputParamsObject)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_OBJECT,)

    @classmethod
    def simple_analyse(cls,
                       **kwargs) -> Generator["OracleOnlineObjectIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        with make_session() as session:
            cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            rule_jar: [CMDBRule] = cls.generate_rule_jar()
            cmdb_connector = cmdb.build_connector()
            for the_rule in rule_jar:
                ret = the_rule.run(
                    entries=rule_jar.entries,

                    cmdb=cmdb,
                    task_record_id=task_record_id,
                    cmdb_connector=cmdb_connector,
                    schema_name=schema_name
                )
                docs = cls.pack_rule_ret_to_doc(the_rule, ret)
                cls.post_analysed(
                    docs=docs,
                    task_record_id=task_record_id,
                    schema_name=schema_name
                )
                yield from docs
