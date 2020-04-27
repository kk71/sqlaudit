# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select * from (
        select a.owner, 
                a.index_name, 
                decode(
                    INDEX_TYPE,'NORMAL','B-tree',decode(index_type,'BITMAP','BitMap',decode(INDEX_TYPE,'FUNCTION-BASED NORMAL','Func',decode(table_type,'IOT','IOT',decode(index_type, 'LOB', 'Lob'))))
                ) as type,
                a.clustering_factor,
                a.table_name,
                b.num_rows,
                trunc((a.clustering_factor - a.leaf_blocks) /(a.num_rows - a.leaf_blocks + 1) * 100,2) ratio
            from dba_ind_statistics a, dba_tables b, dba_indexes c 
            where a.table_name = b.table_name 
                and a.index_name = c.index_name 
                and a.num_rows > 200000 
                and ((a.clustering_factor - a.leaf_blocks) / (a.num_rows - a.leaf_blocks + 1) > 0.7 or a.blevel > 4) 
                and a.index_name not like '%BIN$%' 
                and a.owner = '{schema_name}'
            order by 1, 6) 
        where ratio > {rule.gip('cluster_row_ratio')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
