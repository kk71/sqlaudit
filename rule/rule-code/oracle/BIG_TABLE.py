# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select * 
        from (
            select segment_name as table_name,
                    round(sum(bytes) / 1024 / 1024 / 1024, 2) as tab_phy_size 
                from dba_segments 
                where segment_type = 'TABLE' 
                    and owner = '{schema_name}'
                    and segment_name not like 'BIN$%'  
                group by segment_name
            ) a  
        where tab_phy_size > {rule.gip('tab_phy_size')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
