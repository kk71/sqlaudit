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


def get_authed_cmdbs(login_user):

    sql = "SELECT cmdb_id, schema_name FROM T_DATA_PRIVILEGE"
    sql_params = []

    if login_user != "admin":
        sql += " WHERE login_user = :1"
        sql_params.append(login_user)
    else:
        sql = "SELECT cmdb_id FROM T_CMDB"
    privileges = OracleHelper.select(sql, sql_params, one=False)
    return privileges


def get_rules(rule_type="all"):
    sql = "select RULE_NAME, RISK_SQL_DIMENSION FROM T_RISK_SQL_RULE"
    rules = OracleHelper.select(sql=sql, one=False)
    sql_text = [x[0] for x in rules if x[1] == 'SQL文本']
    sql_plan = [x[0] for x in rules if x[1] == 'SQL执行计划']
    sql_stat = [x[0] for x in rules if x[1] == 'SQL执行效率']
    obj_rules = [x[0] for x in rules if x[1] == '对象']
    return {
        'sql': sql_text,
        'plan': sql_plan,
        'obj': obj_rules,
        'stat': sql_stat,
        'all': (sql_text, sql_plan, sql_stat, obj_rules),
        'both': (sql_text, sql_plan, sql_stat)
    }[rule_type]

