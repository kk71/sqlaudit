# Author: kk.Fang(fkfkbill@gmail.com)


def execute_rule(**kwargs):
    username = kwargs.get("username")
    db_cursor = kwargs.get("db_cursor")
    max_score = kwargs.get("max_score")
    tab_phy_size = kwargs.get("tab_phy_size")

    sql = f"""
select a.owner, a.table_name, b.bytes / 1024 / 1024 size_mb
  from dba_tables a, dba_segments b
 where a.table_name = b.segment_name
   and b.segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
   AND b.owner = '{username}'
   AND b.segment_name NOT LIKE 'BIN%'
   and b.bytes / 1024 / 1024 >= '{tab_phy_size}'
   and a.table_name not in (select table_name from dba_indexes)
    """
    db_cursor.execute(sql)
    ret = list(db_cursor.fetchall())
    if ret:
        return ret, float("%0.2f" % max_score)
    return [], 0.0
