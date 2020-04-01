import re


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(object_type="VIEW",
                               object_owner__ne=None,
                               object_name=re.compile(r"^index$_join$"))
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
