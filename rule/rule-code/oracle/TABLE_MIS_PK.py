# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select a.table_name 
        from dba_tables a 
        where a.owner = '{schema_name}'
            and a.table_name not like 'BIN$%' 
        minus select b.table_name 
            from dba_constraints b 
            where b.owner = '{schema_name}'
                and b.table_name not like 'BIN$%' 
                and b.constraint_type = 'P'
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
