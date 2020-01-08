import re
from utils.const import SQL_DDL


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    sql_type: int = single_sql['sql_type']

    if sql_type != SQL_DDL:
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
