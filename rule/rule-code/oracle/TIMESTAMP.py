# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select table_name 
        from dba_tables 
        where owner = '{schema_name}' 
            and table_name not in (
                select table_name 
                    from dba_tab_cols 
                    where owner = '{schema_name}' and (
                            column_name like 'CREATE%' or column_name like 'UPDATE%'
                        ) and data_type = 'DATE')
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
