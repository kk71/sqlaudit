import re


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    plans = sql_plan_qs.filter(
        object_type="VIEW",
        object_owner__ne=None,
        object_name=re.compile(r"^index$_join$")
    )
    for x in plans:
        yield {}


code_hole.append(code)
