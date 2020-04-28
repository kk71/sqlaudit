# Author: kk.Fang(fkfkbill@gmail.com)

from rule.code_utils import *


def code(rule, entries, **kwargs):
    sql_stat_qs = kwargs["sql_stat_qs"]

    qs = sql_stat_qs.filter(per_disk_reads__gte=rule.gip("disk_reads"))

    for d in values_dict(qs,
                         "sql_id",
                         "plan_hash_value"):
        yield d


code_hole.append(code)
