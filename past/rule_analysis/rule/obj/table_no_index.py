# Author: kk.Fang(fkfkbill@gmail.com)


def execute_rule(**kwargs):
    username = kwargs.get("username")
    db_cursor = kwargs.get("db_cursor")
    max_score = kwargs.get("max_score")
    tab_phy_size = kwargs.get("tab_phy_size")

    sql_table_name_bigger_than_threshold = """
    SELECT segment_name
  FROM dba_segments
 WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
   AND owner = '@username@'
   AND segment_name NOT LIKE 'BIN%'
   and bytes / 1024 / 1024 >= @tab_phy_size@
 ORDER BY 3 DESC
    """
    db_cursor.execute(sql_table_name_bigger_than_threshold.
        replace("@username@", username).replace("@tab_phy_size@", tab_phy_size))
    big_table_table_names = tuple(db_cursor.fetchall())

    sql = f"""
    SELECT 'COMBINEINDEX',
           COUNT(DISTINCT IC.INDEX_NAME) AS COMBINEINDEXNUMBER,
           distinct ic.table_name
    FROM DBA_IND_COLUMNS IC
    WHERE IC.INDEX_OWNER = '@username@'
      AND IC.COLUMN_POSITION > 1
      UNION ALL
      SELECT 'ALLINDEX',
             COUNT(1)
      FROM DBA_INDEXES I WHERE I.OWNER = '@username@' 
      and table_name in {big_table_table_names}
    """
    db_cursor.execute(sql.replace("@username@", username))
    records_comindnum_allinnum = db_cursor.fetchall()

    all_index_sum = 0
    table_names = []
    # 取组合索引数量和所有索引数量并赋值给本地变量
    for i in records_comindnum_allinnum:
        if i[0] == 'COMBINEINDEX':
            pass
        elif i[0] == 'ALLINDEX':
            all_index_sum += i[1]
            table_names.append(i[2])
        else:
            pass

    # 如果所有索引数量为0，则直接返回，并扣除本规则所有分数。
    if True and all_index_sum == 0:
        return [[i] for i in table_names], float("%0.2f" % max_score)
    return [], 0.0
