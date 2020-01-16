import re


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    cross_outer_join = re.compile(r"(cross join)|(outer join)")

    if cross_outer_join.search(sql_text):
        return -rule.weight, []
    return None, []


code_hole.append(code)
