import re
from .utils import judge_ddl


def code(rule, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text"]
    cmdb = kwargs.get("cmdb")
    db_model = cmdb.db_model

    if not judge_ddl(sql_text):
        return None, []

    if db_model == "OLTP" and re.search("create\s+bitmap\s+index", sql_text, re.I):
        return -rule.weight, []
    return None, []


code_hole.append(code)
