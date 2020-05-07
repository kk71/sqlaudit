# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    SELECT segment_name as table_name, segment_type, bytes / 1024 / 1024 tab_space
        FROM dba_segments
        WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
            AND owner = '{schema_name}'
            AND segment_name NOT LIKE 'BIN%'
            and bytes / 1024 / 1024 >= {rule.gip('tab_phy_size')}
        ORDER BY 3 DESC
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
