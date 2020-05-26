# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineSQLIssue",
    "OracleOnlineIssueOutputParamsSQL"
]

from typing import Optional
from collections import defaultdict

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from models.mongoengine import *
from issue.issue import OnlineIssueOutputParams
from oracle_cmdb.issue.base import *
from ...capture import OracleSQLStatToday, OracleSQLText, OracleSQLPlanToday
from ...capture.base.sql import *


class OracleOnlineIssueOutputParamsSQL(OnlineIssueOutputParams):
    """针对SQL的输出字段"""

    sql_id = StringField(required=True, default=None)

    meta = {
        "allow_inheritance": True
    }


class OracleOnlineSQLIssue(OracleOnlineIssue):
    """sql问题"""

    output_params = EmbeddedDocumentField(
        OracleOnlineIssueOutputParamsSQL,
        default=OracleOnlineIssueOutputParamsSQL)

    meta = {
        "allow_inheritance": True
    }

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SQL,)

    RELATED_CAPTURE = (OracleSQLText, OracleSQLPlanToday, OracleSQLStatToday)

    @classmethod
    def referred_capture(
            cls,
            capture_model: OracleSQLCapturingDoc,
            **kwargs) -> Optional[mongoengine_qs]:
        super().referred_capture(capture_model, **kwargs)
        issue_qs: mongoengine_qs = kwargs["issue_qs"]

        # task_record_id: schema_name: [sql_id, ...]
        sqls = defaultdict(lambda: defaultdict(list))
        for task_record_id, schema_name, output_params in issue_qs.values_list(
                "task_record_id",
                "schema_name",
                "output_params"):
            sql_id = output_params.sql_id
            if not sql_id:
                continue
            l = sqls[task_record_id][schema_name]
            if sql_id not in l:
                l.append(sql_id)
        if not sqls:
            return
        q = Q()
        for task_record_id, i1 in sqls.items():
            for schema_name, sql_ids in i1.items():
                q = q | Q(
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    sql_id__in=sql_ids
                )
        return capture_model.filter(q)

    @classmethod
    def referred_capture_distinct(
            cls,
            capture_model: OracleSQLCapturingDoc,
            **kwargs) -> list:
        ret = cls.referred_capture(capture_model, **kwargs)
        if ret is None:
            return []
        return ret.distinct("sql_id")

