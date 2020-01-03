import re
from .utils import judge_ddl


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if not judge_ddl(sql_text):
        return None, []

    if not re.search(r"create\s+table", sql_text, re.I):
        return None, []

    if 'parallel' not in sql_text:
        return None, []
    parallel = re.search("parallel\s+(\d)", sql_text, re.I)
    if parallel and int(parallel.groups(1)) > 1:
        return -rule.weight, []
    return None, []


code_hole.append(code)
