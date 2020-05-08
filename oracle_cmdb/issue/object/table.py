# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleOnlineObjectIssueTable"
]

from collections import defaultdict

import rule.const
from models.mongoengine import *
from .object import *
from ...capture import OracleObjTabCol, OracleObjTabInfo, \
    OracleObjPartTabParent, OracleObjectCapturingDoc


class OracleOnlineObjectIssueTable(OracleOnlineObjectIssue):

    ENTRIES = (rule.const.RULE_ENTRY_ONLINE_TABLE,)

    RELATED_CAPTURE = (
        OracleObjTabCol,
        OracleObjTabInfo,
        OracleObjPartTabParent
    )

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.filter_with_entries(*args, **kwargs)

    @classmethod
    def referred_capture(
            cls,
            capture_model: OracleObjectCapturingDoc,
            **kwargs) -> mongoengine_qs:
        super().referred_capture(capture_model, **kwargs)
        issue_qs: mongoengine_qs = kwargs["issue_qs"]

        # task_record_id: schema_name: [(sequence_owner, sequence_name), ]
        obj = defaultdict(lambda: defaultdict(list))
        for task_record_id, schema_name, output_params in issue_qs.values_list(
                "task_record_id",
                "schema_name",
                "output_params"):
            index_unique_key = (
                output_params.table_name,
            )
            if not all(index_unique_key):
                continue
            l = obj[task_record_id][schema_name]
            if index_unique_key not in l:
                l.append(index_unique_key)
        q = Q()
        for task_record_id, i1 in obj.items():
            for schema_name, obj_unique_key in i1.items():
                q = q | Q(
                    task_record_id=task_record_id,
                    schema_name=schema_name,
                    owner=obj_unique_key[0],
                    table_name=obj_unique_key[1]
                )
        return capture_model.filter(q)

    @classmethod
    def referred_capture_distinct(
            cls,
            capture_model: OracleObjectCapturingDoc,
            **kwargs) -> list:
        return cls.referred_capture(capture_model, **kwargs).distinct("table_name")

