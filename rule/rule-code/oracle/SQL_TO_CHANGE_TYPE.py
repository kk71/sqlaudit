import re


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        filter_predicates=re.compile(r"(SYS_OP|TO_NUMBER|INTERNAL_FUNCTION)", re.I)
    )

    for x in plans:
        yield {}


code_hole.append(code)
