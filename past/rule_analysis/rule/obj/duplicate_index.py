# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict


def execute_rule(**kwargs):
    username = kwargs.get("username")
    db_cursor = kwargs.get("db_cursor")

    sql = """
    SELECT T.TABLE_OWNER||'.'||T.TABLE_NAME, T.COLUMN_NAME, COUNT(T.INDEX_NAME) 
    FROM DBA_IND_COLUMNS T 
    WHERE T.INDEX_OWNER = '@username@' 
    GROUP BY T.TABLE_OWNER||'.'||T.TABLE_NAME, T.COLUMN_NAME 
    HAVING COUNT(T.INDEX_NAME) > 1
    """
    db_cursor.execute(sql.replace("@username@", username))
    records = db_cursor.fetchall()

    # 获取字段重复索引的索引名
    sql = """
    select index_name, table_owner, table_name, column_name
    from dba_ind_columns
    WHERE T.INDEX_OWNER = '@username@' 
    """
    db_cursor.execute(sql.replace("@username@", username))
    index_records = db_cursor.fetchall()
    # table_owner: table_name: column_name:: {index_name}
    column_name_index_name_dict = \
        defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for table_owner_table_name, column_name, _ in records:
        table_owner, table_name = table_owner_table_name.split(".")
        for the_index_name, \
            the_table_owner, \
            the_table_name, \
            the_column_name in index_records:
            if the_table_owner == table_owner and \
                    the_table_name == table_name and \
                    the_column_name == column_name:
                column_name_index_name_dict[table_owner][table_name][column_name].add(
                    the_index_name)
    for record in records:
        table_owner_table_name, column_name, _ = records
        table_owner, table_name = table_owner_table_name.split(".")
        record.append(list(column_name_index_name_dict[table_owner][table_name][column_name]))
    return records, True
