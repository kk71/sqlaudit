def code(rule, entries, **kwargs):

    sql_plan_qs = kwargs["sql_plan_qs"]
    if sql_plan_qs.filter(object_type="TABLE").count() >=\
            rule.gip("tab_num"):
        yield {}


code_hole.append(code)
