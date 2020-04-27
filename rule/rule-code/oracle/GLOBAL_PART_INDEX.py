# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    SELECT X.INDEX_NAME, X.TABLE_NAME 
        FROM DBA_INDEXES X, DBA_PART_TABLES Y 
        WHERE X.OWNER = Y.OWNER 
            AND X.TABLE_NAME = Y.TABLE_NAME  
            AND X.PARTITIONED = 'NO' 
            and X.OWNER = '{schema_name}'
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
