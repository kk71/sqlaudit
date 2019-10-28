# Author: kk.Fang(fkfkbill@gmail.com)

import re


def execte_rule(sql, db_model, **kwargs):

    if not re.search(r"create\s+table", sql, re.I) and not re.search(r"alter\s+table", sql, re.I):
        return True

    if any([x in sql.lower() for x in ['blob', 'clob', 'bfile', 'xmltype']]):
        return "高频表上不推荐使用LOB字段"

    return True

