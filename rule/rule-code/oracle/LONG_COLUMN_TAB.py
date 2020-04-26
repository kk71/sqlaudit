# Author: kk.Fang(fkfkbill@gmail.com)


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = """
    SELECT CASE userenv('language')
               WHEN 'SIMPLIFIED CHINESE_CHINA.AL32UTF8' THEN 3
               WHEN 'AMERICAN_AMERICA.AL32UTF8' THEN 3
               WHEN 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK' THEN 4
               WHEN 'AMERICAN_AMERICA.ZHS16GBK' THEN 4
               WHEN 'SIMPLIFIED CHINESE_CHINA.UTF8' THEN 3
           END
    FROM dual
    """
    v_len = cmdb_connector.select(sql)
    sql = f"""
    SELECT t.table_name,
           a.col_sum,
           t.avg_row_len
    FROM dba_tables t,

      (SELECT TABLE_NAME,
              sum(LENGTH) col_sum
       FROM
         (SELECT TABLE_NAME,
                 data_length,
                 COLUMN_NAME,
                 sum(CASE data_type
                         WHEN 'VARCHAR2' THEN round(data_length / 1, 2)
                         WHEN 'VARCHAR' THEN round(data_length / 1, 2)
                         ELSE data_length
                     END) LENGTH
          FROM dba_tab_cols
          WHERE OWNER='{schema_name}'
          GROUP BY TABLE_NAME,
                   data_length,
                   COLUMN_NAME) t
       GROUP BY TABLE_NAME) a
    WHERE t.owner='{schema_name}'
      AND t.table_name = a.table_name
      AND t.avg_row_len / a.col_sum < {rule.gip('data_len_ratio')}
    """
    records = cmdb_connector.select_dict(sql)

    # 新增功能，返回超过实际长度的字段名
    sql = f"""
                select table_name, column_name, data_length 
                from dba_tab_cols t
                where t.owner = '{schema_name}'
    """
    table_columns = cmdb_connector.select(sql)
    for record in records:
        record["column_names"] = set()
        for the_table_name, the_column_name, the_data_length in table_columns:
            if record["table_name"] == the_table_name:
                if the_data_length >= record["avg_row_len"]:
                    record["column_names"].add(the_column_name)
        yield record


code_hole.append(code)
