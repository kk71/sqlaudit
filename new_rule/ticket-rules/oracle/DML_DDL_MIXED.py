
def code(rule, entries, **kwargs):
    """判断同一个工单的sql语句是否存在ddl和dml混写的情况"""
    sqls: [dict] = kwargs.get("sqls")

    sql_types: set = {i["sql_type"] for i in sqls}
    if len(sql_types) > 1:
        yield {}


code_hole.append(code)
