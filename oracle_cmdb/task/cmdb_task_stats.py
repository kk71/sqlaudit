# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBTaskStatsSnapIDPairs",
    "OracleCMDBTaskStatsEntriesAndRules",
    "OracleCMDBTaskStatsReferredTaskRecordID"
]

from mongoengine import ListField, StringField

import cmdb.cmdb_task_stats


class OracleCMDBTaskStats(cmdb.cmdb_task_stats.CMDBTaskStats):

    meta = {
        "allow_inheritance": True
    }


class OracleCMDBTaskStatsSnapIDPairs(OracleCMDBTaskStats):
    """snap shot id pairs"""

    snap_shot_id_pair = ListField(default=list)


class OracleCMDBTaskStatsEntriesAndRules(OracleCMDBTaskStats):
    """每次使用的entries以及涉及的规则unique_key"""

    schema_name = StringField(required=True)
    entries = ListField(default=list)
    rule_info = ListField(default=list)


class OracleCMDBTaskStatsReferredTaskRecordID(OracleCMDBTaskStats):
    """跨库或者跨任务级别的统计，依赖的task_record_id"""

    referred_task_record_id_list = ListField(default=list)
