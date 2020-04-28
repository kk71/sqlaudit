# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    with tc as (
        select table_name, column_name 
            from dba_cons_columns 
            where owner||'.'||constraint_name in (
                select owner||'.'||constraint_name 
                    from dba_constraints 
                    where owner = '{schema_name}' 
                        and constraint_type = 'R')  
                        and owner = '{schema_name}')
    select table_name, column_name  
        from tc  
        where (table_name, column_name) not in (
            select table_name, column_name  
                from tc intersect   select table_name, column_name 
                    from dba_ind_columns  
                    where index_owner = '{schema_name}')
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
