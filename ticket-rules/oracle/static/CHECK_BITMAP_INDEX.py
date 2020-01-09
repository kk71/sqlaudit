import re
from utils.const import SQL_DDL, MODEL_OLTP


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    sql_type: int = single_sql['sql_type']
    cmdb = kwargs.get("cmdb")
    db_model = cmdb.db_model

    if sql_type != SQL_DDL:
        return None, []

    if db_model == MODEL_OLTP and\
            re.search(r"create\s+bitmap\s+index", sql_text, re.I):
        return -rule.weight, []
    return None, []


code_hole.append(code)
