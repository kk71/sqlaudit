import re
from cmdb.const import MODEL_OLTP


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]
    cmdb = kwargs.get("cmdb")
    db_model = cmdb.db_model

    if db_model == MODEL_OLTP and\
            re.search(r"create\s+bitmap\s+index", sql_text, re.I+re.M):
        yield {"sql_id": kwargs.get("sql_id", None)}


code_hole.append(code)
