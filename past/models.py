# -*- coding: utf-8 -*-

from plain_db.oracleob import OracleHelper


def get_cmdb(cmdb_id=None, select=["*"], return_type="dict"):

    sql = f"SELECT {', '.join(select)} FROM T_CMDB"
    if cmdb_id:
        sql += f" WHERE cmdb_id = {cmdb_id}"
    if return_type == "dict":
        return OracleHelper.select_dict(sql, one=True if cmdb_id else False)
    else:
        return OracleHelper.select(sql, one=True if cmdb_id else False)


def get_risk_rules(select=["*"], return_type="dict"):

    sql = f"SELECT {', '.join(select)} FROM T_RISK_SQL_RULE"

    if return_type == "dict":
        return OracleHelper.select_dict(sql, one=False)
    else:
        return OracleHelper.select(sql, one=False)


def get_top_cmdb_ids(login_user=None, select=["*"]) -> list:

    sql = "SELECT top_cmdb_ids FROM T_USER WHERE login_user = :1"
    res = OracleHelper.select(sql, [login_user], one=True)[0] or ""
    top_cmdb_ids = [int(x) for x in res.split(',') if x]
    return top_cmdb_ids


def get_rules(rule_type="all"):
    sql = "select RULE_NAME, RULE_TYPE FROM T_RISK_SQL_RULE"
    rules = OracleHelper.select(sql=sql, one=False)
    sql_text = [x[0] for x in rules if x[1] == 'SQL文本' or 'TEXT']
    sql_plan = [x[0] for x in rules if x[1] == 'SQL执行计划'or 'SQLPLAN']
    sql_stat = [x[0] for x in rules if x[1] == 'SQL执行效率' or 'SQLATAT']
    obj_rules = [x[0] for x in rules if x[1] == '对象' or 'OBJ']
    return {
        'sql': sql_text,
        'plan': sql_plan,
        'obj': obj_rules,
        'stat': sql_stat,
        'all': (sql_text, sql_plan, sql_stat, obj_rules),
        'both': (sql_text, sql_plan, sql_stat)
    }[rule_type]

