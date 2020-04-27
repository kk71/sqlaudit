# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select t.table_name, count(1) as part_tab_num
        from DBA_TAB_PARTITIONS t 
        where t.table_owner = '{schema_name}'
        group by t.table_name 
        having count(1) >= {rule.gip('part_tab_num')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
