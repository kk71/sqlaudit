import re
from .utils import judge_ddl


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]

    if not judge_ddl(sql_text):
        return None, []
    if re.search('create\s+index', sql_text, re.I) and 'tablespace' not in sql_text:
        return -rule.weight, []
    return None, []


code_hole.append(code)
