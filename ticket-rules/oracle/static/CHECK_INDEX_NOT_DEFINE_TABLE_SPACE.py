import re
from utils.const import SQL_DDL


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    sql_type: int = single_sql['sql_type']

    if sql_type != SQL_DDL:
        return None, []

    if re.search(r'create\s+index', sql_text, re.I) and 'tablespace' not in sql_text:
        return -rule.weight, []
    return None, []


code_hole.append(code)
