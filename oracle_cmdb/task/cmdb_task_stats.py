# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBTaskStatsSnapIDPairs",
    "OracleCMDBTaskStatsEntriesAndRules"
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
    rule_unique_keys = ListField(default=list)
