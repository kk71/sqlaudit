# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueSequence"
]

from typing import Optional, Tuple
from collections import defaultdict

from mongoengine import StringField, EmbeddedDocumentField

import rule.const
from ... import const
from models.mongoengine import *
from .object import *
from ..base import OracleOnlineIssue
from ...capture import OracleObjSeqInfo, OracleObjectCapturingDoc
from issue.issue import OnlineIssueOutputParams


class OnlineIssueOutputParamsObjectSequence(OnlineIssueOutputParams):

    sequence_name = StringField(null=True)


@OracleOnlineIssue.need_collect()
class OracleOnlineObjectIssueSequence(OracleOnlineObjectIssue):
    """对象问题: 序列"""

    output_params = EmbeddedDocumentField(
        OnlineIssueOutputParamsObjectSequence,
        default=OnlineIssueOutputParamsObjectSequence)

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_SEQUENCE,)

    RELATED_CAPTURE = (OracleObjSeqInfo,)

    @classmethod
    def referred_capture(
            cls,
            capture_model: OracleObjectCapturingDoc,
            **kwargs) -> Optional[mongoengine_qs]:
        super().referred_capture(capture_model, **kwargs)
        issue_qs: mongoengine_qs = kwargs["issue_qs"]

        # task_record_id: schema_name: [(sequence_owner, sequence_name), ]
        obj = defaultdict(lambda: defaultdict(list))
        for task_record_id, schema_name, output_params in issue_qs.values_list(
                "task_record_id",
                "schema_name",
                "output_params"):
            index_unique_key = output_params.sequence_name
            if not index_unique_key:
                continue
            l = obj[task_record_id][schema_name]
            if index_unique_key not in l:
                l.append(index_unique_key)
        if not obj:
            return
        q = Q()
        for task_record_id, i1 in obj.items():
            for schema_name, obj_unique_key in i1.items():
                q = q | Q(
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    sequence_name__in=obj_unique_key
                )
        return capture_model.filter(q)

    @classmethod
    def referred_capture_distinct(
            cls,
            capture_model: OracleObjectCapturingDoc,
            **kwargs) -> list:
        ret = cls.referred_capture(capture_model, **kwargs)
        if ret is None:
            return []
        return ret.distinct("sequence_name")

    def get_object_unique_name(self) -> Tuple[Optional[str], str, str]:
        return self.schema_name, \
               const.ORACLE_OBJECT_TYPE_SEQUENCE, \
               self.output_params.sequence_name

    def get_referred_table_name(self) -> Optional[str]:
        return None
