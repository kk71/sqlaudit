# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select t.table_name, t.partition_name, count(1) as subpart_count
        from DBA_TAB_SUBPARTITIONS t  
        where t.table_owner = '{schema_name}'
        group by t.table_name, t.partition_name 
        having count(1) >= {rule.gip('subpart_tab_num')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
