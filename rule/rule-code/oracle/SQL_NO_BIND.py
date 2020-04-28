# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    cmdb_connector = kwargs["cmdb_connector"]
    schema_name: str = kwargs["schema_name"]
    sqls = kwargs["sqls"]

    # 拼接sql_id批量查询
    sql_ids = [i["sql_id"] for i in sqls]
    sql_ids_text = ""
    if sql_ids:
        sql_ids_text = ",".join([f"'{sql_id}'" for sql_id in sql_ids])

    sql = f"""
    select t.sql_id, 
            t.force_matching_signature, 
            count(*) sql_no_bind_count
        from dba_hist_sqlstat t, dba_hist_sqlstat x
        where t.force_matching_signature = x.force_matching_signature
            and t.PARSING_SCHEMA_NAME = '{schema_name}'
            and t.sql_id in ({sql_ids_text})
        group by t.sql_id, t.plan_hash_value, t.force_matching_signature
        having count(*) >= {rule.gip('sql_no_bind_count')}
        order by 4 desc
"""
    for i in cmdb_connector.select_dict(sql):
        yield i


code_hole.append(code)
