# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select t.table_name, 
            decode(t.partitioned, 'YES', 'PART',  decode(t.temporary, 'Y', 'TEMP', decode (t.iot_type,'IOT','IOT','NORMAL'))) table_type,
            t.degree parallel_count
        from dba_tables t  
        where t.table_name not like 'BIN%'  
            and t.owner = '{schema_name}'
            and to_number(replace(ltrim(t.degree),'DEFAULT',0))  > 1 
        order by table_type, table_name
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
