import re

from rule import const
from rule.code_utils import *


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    qs = sql_plan_qs.filter(
        filter_predicates=re.compile(
            r"(SYS_OP|TO_NUMBER|INTERNAL_FUNCTION)", re.I)
    )

    if const.RULE_ENTRY_TICKET_DYNAMIC in entries:
        for d in values_dict(qs, "object_name", "object_type"):
            yield d

    elif const.RULE_ENTRY_ONLINE_SQL_PLAN in entries:
        for d in values_dict(qs,
                             "sql_id",
                             "plan_hash_value",
                             "object_name",
                             "object_type"):
            yield d


code_hole.append(code)
