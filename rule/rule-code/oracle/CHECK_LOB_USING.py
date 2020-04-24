import re


def code(rule, entries, **kwargs):
    single_sql: dict = kwargs.get("single_sql")
    sql_text: str = single_sql["sql_text_no_comment"]

    if (
            not re.search(r"create\s+table", sql_text, re.I)
            and not re.search(r"alter\s+table", sql_text, re.I)
    ) or any([
        x in sql_text.lower()
        for x in ['blob', 'clob', 'bfile', 'xmltype']
    ]):
        yield {"sql_id": kwargs.get("sql_id", None)}


code_hole.append(code)
