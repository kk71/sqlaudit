# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select a.table_name, b.bytes / 1024 / 1024 table_size
        from dba_tables a, dba_segments b
        where a.table_name = b.segment_name
            and b.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
            AND b.owner = '{schema_name}'
            AND b.segment_name NOT LIKE 'BIN%'
            and b.bytes / 1024 / 1024 >= {rule.gip('tab_phy_size')}
            and a.table_name not in (
                select table_name from dba_indexes)
"""
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
