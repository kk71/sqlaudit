# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    SELECT T.TABLE_NAME table_name, COUNT(1) index_num FROM DBA_INDEXES T
           WHERE T.OWNER = '{schema_name}'
           GROUP BY T.TABLE_NAME 
           HAVING COUNT(1) > {rule.gip("index_num")} ORDER BY COUNT(1) DESC
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
