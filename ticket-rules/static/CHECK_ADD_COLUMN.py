import re
from utils.const import SQL_DDL


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    sql_type: int = single_sql['sql_type']

    if sql_type != SQL_DDL:
        return None, []

    if re.search('alter\s+table\s+.+\s+add', sql_text, re.I):
        if not re.search(".+\s+default\s+.+", sql_text, re.I):
            return -rule.weight, []
    return None, []


code_hole.append(code)
