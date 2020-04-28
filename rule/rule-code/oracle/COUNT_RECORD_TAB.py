# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select t.table_name,
            t.num_rows 
        from DBA_TABLES t  
        where t.partitioned='NO' 
            and t.owner='{schema_name}' 
            and t.num_rows > {rule.gip('record_count')}
        union 
            select t.table_name||'.'||t.table_name||':'||t.PARTITION_NAME,
                    t.num_rows  
                from  dba_tab_PARTITIONS t  
                where t.table_owner='{schema_name}'
                    and t.num_rows > {rule.gip('record_count')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
