# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select table_name, 
            count(1) column_count
        from dba_tab_cols
        where owner = '{schema_name}'
        group by table_name
        having count(1) > {rule.gip('column_count')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
