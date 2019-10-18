# Author: kk.Fang(fkfkbill@gmail.com)

import re



def execute_rule(mongo_client, sql, username, etl_date_key, etl_date, **kwargs):

    if re.search('create\s+database\s+link', sql, re.I):
        return False, "不建议创建DB LINK"
    return True, None

