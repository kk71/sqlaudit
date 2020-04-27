# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]
    snap_id_s, snap_id_e = kwargs["snap_ids"]

    sql = f"""
        SELECT sql_id, plan_hash_value, version_count
        FROM
          (SELECT t.parsing_schema_name AS username,
                  t.sql_id,
                  t.plan_hash_value,
                  t.version_count,
                  row_number() over(partition BY t.sql_id
                                    ORDER BY t.plan_hash_value DESC) rank
           FROM dba_hist_sqlstat t
           WHERE t.parsing_schema_name = '{schema_name}'
             AND (t.snap_id BETWEEN '{snap_id_s}' AND '{snap_id_e}')) e
        WHERE e.rank = 1 and e.version_count >= {rule.gip('cursor_num')}
    """
    for d in cmdb_connector.select_dict(sql):
        yield d


code_hole.append(code)
