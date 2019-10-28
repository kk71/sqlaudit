# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execte_rule(sql, username, etl_date_key, etl_date, **kwargs):

    if not re.search(r"create\s+table", sql, re.I) and not re.search(r"alter\s+table", sql, re.I):
        return True, None

    if any([x in sql.lower() for x in ['blob', 'clob', 'bfile', 'xmltype']]):
        return False, "高频表上不推荐使用LOB字段"

    return True, None

