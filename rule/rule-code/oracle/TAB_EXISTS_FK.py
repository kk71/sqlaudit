# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select c.table_name,  
            c.constraint_name,  
            t.column_name, 
            (select distinct a.table_name  
                from dba_constraints a  
                where a.constraint_name = c.r_constraint_name 
                    and a.owner = c.owner) rel_tab_name,
            c.r_constraint_name rel_con_name
        from dba_constraints c, dba_cons_columns t  
        where t.owner = c.owner  
            and t.constraint_name = c.constraint_name 
            and c.owner = '{schema_name}'   
            and c.table_name not like 'BIN$%'  
            and c.constraint_type = 'R'
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
