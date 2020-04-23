from parsed_sql import const


def code(rule, entries, **kwargs):
    sqls: [dict] = kwargs.get("sqls")

    sql_types: set = {i["sql_type"] for i in sqls}
    if const.SQL_DDL in sql_types and const.SQL_DML in sql_types:
        yield {}


code_hole.append(code)
