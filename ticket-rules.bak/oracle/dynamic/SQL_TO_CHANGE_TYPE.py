import re


def code(rule, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        filter_predicates=re.compile(r"(SYS_OP|TO_NUMBER|INTERNAL_FUNCTION)", re.I)
    )

    for x in plans:
        return -rule.weight, [
            x.statement_id,
            x.plan_id,
            x.object_name,
            x.the_id,
            x.cost
        ]
    return None, []


code_hole.append(code)