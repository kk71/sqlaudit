import re


def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        operation="TABLE ACCESS",
        options=re.compile(r"BY GLOBAL INDEX ROWID", re.I)
    )
    if plans.count():
        return -rule.weight, []
    return None, []


code_hole.append(code)
