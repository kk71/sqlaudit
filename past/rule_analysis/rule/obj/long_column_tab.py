# -*- coding: utf-8 -*-


def execute_rule(**kwargs):
    db_cursor = kwargs.get("db_cursor")
    data_len_ratio = kwargs.get("data_len_ratio")
    username = kwargs.get("username")
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
    db_cursor.execute(sql)
    v_len = db_cursor.fetchall()
    sql = """
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
          WHERE OWNER='@username@'
          GROUP BY TABLE_NAME,
                   data_length,
                   COLUMN_NAME) t
       GROUP BY TABLE_NAME) a
    WHERE t.owner='@username@'
      AND t.table_name = a.table_name
      AND t.avg_row_len / a.col_sum < @data_len_ratio@
    """
    sql = sql.replace("@v_len@", str(v_len[0][0])).\
        replace("@username@", username).\
        replace("@data_len_ratio@", str(data_len_ratio))
    db_cursor.execute(sql)
    records = list(db_cursor.fetchall())
    # 新增功能，返回超过实际长度的字段名
    sql = f"""select table_name, column_name, data_length from dba_tab_cols t
             where t.owner = '{username}'"""
    db_cursor.execute(sql)
    table_columns = db_cursor.fetchall()
    for record in records:
        table_name, data_length, avg_col_len = record
        record.append([])
        column_names = record[3]
        for the_table_name, the_column_name, the_data_length in table_columns:
            if table_name == the_table_name:
                if the_data_length > avg_col_len:
                    column_names.append(the_column_name)
    return records, True
