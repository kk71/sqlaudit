# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueIndex"
]

from typing import Optional
from collections import defaultdict

import rule.const
from models.mongoengine import *
from .object import *
from ...capture import OracleObjIndColInfo, OracleObjectCapturingDoc


class OracleOnlineObjectIssueIndex(OracleOnlineObjectIssue):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_INDEX,)

    RELATED_CAPTURE = (OracleObjIndColInfo,)

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.filter_with_entries(*args, **kwargs)

    @classmethod
    def referred_capture(
            cls,
            capture_model: OracleObjectCapturingDoc,
            **kwargs) -> Optional[mongoengine_qs]:
        super().referred_capture(capture_model, **kwargs)
        issue_qs: mongoengine_qs = kwargs["issue_qs"]

        # task_record_id: schema_name: [(index_owner, index_name), ]
        obj = defaultdict(lambda: defaultdict(list))
        for task_record_id, schema_name, output_params in issue_qs.values_list(
                "task_record_id",
                "schema_name",
                "output_params"):
            if getattr(output_params, "index_name", None) is None:
                continue
            index_unique_key = output_params.index_name
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
                    index_name__in=obj_unique_key
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
        return ret.distinct("index_name")
