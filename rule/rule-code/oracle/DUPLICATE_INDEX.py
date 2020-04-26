# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict


def code(rule, entries, **kwargs):
    schema_name: str = kwargs["schema_name"]
    cmdb_connector = kwargs["cmdb_connector"]

    sql = f"""
    SELECT T.TABLE_OWNER, 
            T.TABLE_NAME, 
            T.COLUMN_NAME, 
            COUNT(T.INDEX_NAME) repeat_times
        FROM DBA_IND_COLUMNS T 
        WHERE T.INDEX_OWNER = '{schema_name}'
        GROUP BY T.TABLE_OWNER, T.TABLE_NAME, T.COLUMN_NAME 
        HAVING COUNT(T.INDEX_NAME) > 1
    """
    records = cmdb_connector.select_dict(sql)

    # 获取字段重复索引的索引名
    sql = f"""
    select index_name, table_owner, table_name, column_name
        from dba_ind_columns
        WHERE INDEX_OWNER = '{schema_name}'
    """
    index_records = cmdb_connector.select(sql)

    # table_owner: table_name: column_name:: {index_name}
    column_name_index_name_dict = \
        defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for index_name, table_owner, table_name, column_name in index_records:
        column_name_index_name_dict[table_owner][table_name][column_name].add(index_name)
    for record in records:
        if record.get("referred_index_name", None) is None:
            record["referred_index_name"]: list = []
        index_names = column_name_index_name_dict[record["table_owner"]][record["table_name"]][record["column_name"]]
        record["referred_index_name"].extend(index_names)
        record["referred_index_name"] = list(set(record["referred_index_name"]))

    yield from records


code_hole.append(code)
