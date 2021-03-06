# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssue"
]

from typing import Generator, Tuple, Optional

import rule.const
from models.sqlalchemy import *
from rule.cmdb_rule import CMDBRule
from oracle_cmdb.issue.base import *
from oracle_cmdb.cmdb import *
from ...capture import OracleObjTabCol, OracleObjTabInfo, OracleObjSeqInfo, \
    OracleObjTabSpace, OracleObjIndColInfo, OracleObjViewInfo, OracleObjPartTabParent


class OracleOnlineObjectIssue(OracleOnlineIssue):
    """对象问题"""

    # TODO 请注意，虽然"对象问题"有子类，但是子类并没有囊括全部的"对象问题"！

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_OBJECT,)

    RELATED_CAPTURE = (
        OracleObjTabCol, OracleObjTabInfo, OracleObjSeqInfo,
        OracleObjTabSpace, OracleObjIndColInfo, OracleObjViewInfo,
        OracleObjPartTabParent
    )

    meta = {
        "allow_inheritance": True
    }

    @classmethod
    def simple_analyse(cls,
                       **kwargs) -> Generator["OracleOnlineObjectIssue", None, None]:
        task_record_id: int = kwargs["task_record_id"]
        cmdb_id: int = kwargs["cmdb_id"]
        schema_name: str = kwargs["schema_name"]

        with make_session() as session:
            cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
            rule_jar: [CMDBRule] = cls.generate_rule_jar(
                cmdb_id,
                task_record_id=task_record_id,
                schema_name=schema_name
            )
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

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        """
        获取对象的名称
        :return: [owner, object_type, object_name]
        """
        raise NotImplementedError

    def get_referred_table_name(self) -> Optional[str]:
        """查询对象相关的表名。因为对象查看页面都是根据表名去索引的，因此对象展示的时候需要一个表名"""
        raise NotImplementedError
