# Author: kk.Fang(fkfkbill@gmail.com)

from rule.code_utils import *


def code(rule, entries, **kwargs):
    sql_stat_qs = kwargs["sql_stat_qs"]

    qs = sql_stat_qs.filter(per_cpu_time__gte=rule.gip("cpu_time"))

    for d in values_dict(qs,
                         "sql_id",
                         "plan_hash_value"):
        yield d


code_hole.append(code)
