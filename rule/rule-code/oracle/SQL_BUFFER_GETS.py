# Author: kk.Fang(fkfkbill@gmail.com)

from rule.code_utils import *


def code(rule, entries, **kwargs):
    sql_stat_qs = kwargs["sql_stat_qs"]

    qs = sql_stat_qs.filter(per_buffer_gets__gte=rule.gip("buffer_gets"))

    for d in values_dict(qs,
                         "sql_id",
                         "plan_hash_value",
                         "cpu_time_delta",
                         "execution_delta",
                         "buffer_gets_delta",
                         "disk_reads_delta",
                         "elapsed_time_delta"):
        yield d


code_hole.append(code)
