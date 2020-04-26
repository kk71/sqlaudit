# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    select SEQUENCE_NAME, CACHE_SIZE 
            from dba_sequences 
            WHERE SEQUENCE_OWNER = '{schema_name}'
                  AND CACHE_SIZE < {rule.gip('cache_size')}
    """
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
