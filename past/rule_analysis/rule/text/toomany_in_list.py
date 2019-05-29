# -*- coding: utf-8 -*-

import re


def execute_rule(**kwargs):
    sql = kwargs.get("sql")
    in_list_num = kwargs.get("in_list_num")

    sql_content = [x for x in re.findall(" in\s*\((.*?)\)", sql, re.I) if 'select' not in x]
    for value in sql_content:
        if value.count(",") > in_list_num - 1:
            return True
    return False


if __name__ == "__main__":
    sql = "select object_name from appuser1.objectname1_s a where object_id in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21); in(1, 2, 3)"
    res = execute_rule(sql=sql, in_list_num=20)

