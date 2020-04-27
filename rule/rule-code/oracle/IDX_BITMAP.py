# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select t.INDEX_NAME, t.INDEX_TYPE, t.TABLE_NAME, s.column_name idx_col_name
        from dba_indexes t, dba_ind_columns s 
        where t.owner = s.table_owner 
            and t.index_name = s.index_name 
            and t.index_name not like 'BIN%' 
            and t.owner = '{schema_name}'
            and t.index_type = 'BITMAP' 
        order by t.table_name, t.index_name
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
