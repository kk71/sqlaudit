import re
from utils.const import SQL_DDL


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    sql_type: int = single_sql['sql_type']

    if sql_type != SQL_DDL:
        return None, []

    if not re.search(r"create\s+table", sql_text, re.I) \
            and not re.search(r"alter\s+table", sql_text, re.I):
        return None, []

    if any([x in sql_text.lower() for x in ['blob', 'clob', 'bfile', 'xmltype']]):
        return -rule.weight, []

    return None, []


code_hole.append(code)
