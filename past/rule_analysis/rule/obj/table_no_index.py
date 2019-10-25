# Author: kk.Fang(fkfkbill@gmail.com)


def execute_rule(**kwargs):
    username = kwargs.get("username")
    db_cursor = kwargs.get("db_cursor")
    max_score = kwargs.get("max_score")

    # sql变量，返回传入用户的组合索引数量和所有索引数量
    sql = """
    SELECT 'COMBINEINDEX',
           COUNT(DISTINCT IC.INDEX_NAME) AS COMBINEINDEXNUMBER
    FROM DBA_IND_COLUMNS IC
    WHERE IC.INDEX_OWNER = '@username@'
      AND IC.COLUMN_POSITION > 1
      UNION ALL
      SELECT 'ALLINDEX',
             COUNT(1)
      FROM DBA_INDEXES I WHERE I.OWNER = '@username@'
    """
    db_cursor.execute(sql.replace("@username@", username))
    records_comindnum_allinnum = db_cursor.fetchall()

    # 取组合索引数量和所有索引数量并赋值给本地变量
    for i in records_comindnum_allinnum:
        if i[0] == 'COMBINEINDEX':
            comind_num = i[1]
        if i[0] == 'ALLINDEX':
            allind_num = i[1]

    # 如果所有索引数量为0，则直接返回，并扣除本规则所有分数。
    if allind_num == 0:
        return [], float("%0.2f" % (max_score))
    return [], 0.0
