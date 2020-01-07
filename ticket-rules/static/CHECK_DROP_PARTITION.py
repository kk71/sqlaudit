import re
from .utils import judge_ddl


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if not judge_ddl(sql_text):
        return None, []

    if re.search('alter\s+table\s+.+\s+drop\s+partition', sql_text, re.I):
        return -rule.weight, []
    return None, []


code_hole.append(code)
