# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select object_name as func_name
        from dba_objects 
        where object_type = 'FUNCTION' and owner = '{schema_name}' 
        order by object_type
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
