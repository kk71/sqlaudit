# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select index_name, type as index_type, degree as parallel_count, table_name  
        from (
            select 
                    distinct t.owner||'.'||t.INDEX_NAME as INDEX_NAME ,
                    decode(INDEX_TYPE,  'NORMAL',  'B-tree',  decode(index_type,  'BITMAP',  'BitMap',  decode(INDEX_TYPE,  'FUNCTION-BASED NORMAL', 'Func',  decode(table_type,  'IOT',  'IOT',  decode(index_type,  'LOB',  'Lob'))))) as type,
                    t.TABLE_NAME,  
                    t.degree   
                from dba_indexes t  
                where t.index_name not like 'BIN%'  
                    and t.owner = '{schema_name}'
                    and to_number(replace(ltrim(t.degree),'DEFAULT',0)) >1  
                order by t.table_name, t.owner||'.'||t.index_name
        )
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
