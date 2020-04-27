# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select db_link as dblink_name, username, host as hostname
        from dba_db_links 
        where owner = '{schema_name}'
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
