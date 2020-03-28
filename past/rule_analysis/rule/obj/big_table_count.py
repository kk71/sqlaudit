# -*- coding: utf-8 -*-


def execute_rule(**kwargs):
    username = kwargs.get("username")
    db_cursor = kwargs.get("db_cursor")
    tab_phy_size = kwargs.get("tab_phy_size")
    # weight = kwargs.get("weight")
    max_score = kwargs.get("max_score")
    sql = """
    SELECT segment_name, segment_type, bytes / 1024 / 1024 tab_space
  FROM dba_segments
 WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
   AND owner = '@username@'
   AND segment_name NOT LIKE 'BIN%'
   and bytes / 1024 / 1024 >= @tab_phy_size@
 ORDER BY 3 DESC
    """
    sql = sql.replace("@username@", username).\
        replace("@tab_phy_size@", str(tab_phy_size))
    db_cursor.execute(sql)
    records1 = db_cursor.fetchall()
    # sql = """
    # SELECT count(*)
    # FROM dba_objects t
    # WHERE t.owner='@username@'
    #   AND t.object_type='TABLE'
    # """
    # db_cursor.execute(sql.replace("@username@", username))
    # records2 = db_cursor.fetchall()
    #
    # try:
    #     ratio = float("%0.2f" % float(records1[0][0] / records2[0][0]))
    # except ZeroDivisionError:
    #     ratio = 0
    # if ratio >= float(weight):
    #     scores = max_score
    # else:
    #     scores = 0

    scores = 0
    if len(records1):
        scores = max_score
    if scores > 0:
        return records1, scores
    else:
        return [], 0
