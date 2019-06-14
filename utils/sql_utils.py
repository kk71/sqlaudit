# Author: kk.Fang(fkfkbill@gmail.com)

import re

import sqlparse
from mongoengine import Q

from utils.perf_utils import *
from models.mongo import *
from models.oracle import *
from utils.datetime_utils import *
from utils import rule_utils, cmdb_utils
from utils.const import SQL_DDL, SQL_DML


TICKET_TYPE_STATIC_RULE = {
    SQL_DDL: ["BAD_JOIN", "DML_ALLDATA", "DML_SORT", "LIKE_UNINDEX", "LONG_TEXT", "SELECT_ANY", "SUBQUERY_REP",
              "TOOMANY_BIND", "TOOMANY_IN_LIST", "TOOMANY_OR", "UNION", "WHERE_FUNC", "WHERE_NOT"],
    SQL_DML: ["BAD_JOIN", "DML_ALLDATA", "DML_SORT", "LIKE_UNINDEX", "LONG_TEXT", "SELECT_ANY", "SUBQUERY_REP",
              "TOOMANY_BIND", "TOOMANY_IN_LIST", "TOOMANY_OR", "UNION", "WHERE_FUNC", "WHERE_NOT"]
}
TICKET_TYPE_DYNAMIC_SQLPLAN_RULE = {
    SQL_DML: ["LOOP_IN_TAB_FULL_SCAN", "SQL_INDEX_FAST_FULL_SCAN", "SQL_INDEX_FULL_SCAN", "SQL_INDEX_SKIP_SCAN",
              "SQL_LOOP_NUM", "SQL_MERGE_JOIN_CARTESIAN", "SQL_PARALLEL_FETCH", "SQL_PARTITION_RANGE_ALL",
              "SQL_PARTITION_RANGE_INLIST_OR", "SQL_PARTITION_RANGE_ITERATOR", "SQL_TAB_REL_NUM", "SQL_TABLE_FULL_SCAN",
              "SQL_TO_CHANGE_TYPE", "SQL_VIEW_SCAN"],
    SQL_DDL: ["LOOP_IN_TAB_FULL_SCAN", "SQL_INDEX_FAST_FULL_SCAN", "SQL_INDEX_FULL_SCAN", "SQL_INDEX_SKIP_SCAN",
              "SQL_LOOP_NUM", "SQL_MERGE_JOIN_CARTESIAN", "SQL_PARALLEL_FETCH", "SQL_PARTITION_RANGE_ALL",
              "SQL_PARTITION_RANGE_INLIST_OR", "SQL_PARTITION_RANGE_ITERATOR", "SQL_TAB_REL_NUM", "SQL_TABLE_FULL_SCAN",
              "SQL_TO_CHANGE_TYPE", "SQL_VIEW_SCAN"]
}

RULE_COMMANDS = {
    'SQL_INDEX_FAST_FULL_SCAN': "db.@sql@.find({OPERATION: 'INDEX', OPTIONS: 'FAST FULL SCAN', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})",
    'SQL_INDEX_FULL_SCAN': "db.@sql@.find({OPERATION: 'INDEX', OPTIONS: 'FULL SCAN', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})",
    'SQL_TAB_REL_NUM': "db.@sql@.group( { key:{\"USERNAME\":1,\"record_id\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1,\"OBJECT_TYPE\":1}, cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",\"OBJECT_TYPE\":\"TABLE\"}, reduce:function(curr,result){ result.count++; }, initial:{count:0} } ).forEach(function(x){db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"ID\":x.ID,\"COUNT\":x.count})});db.@tmp1@.find({\"COUNT\":{$gte:@tab_num@}}).forEach(function(y){db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"ID\":y.ID,\"COUNT\":y.COUNT,\"COST\":\"\",\"OBJECT_NAME\":\"\"})})",
    'LOOP_IN_TAB_FULL_SCAN': "db.@sql@.find({$or: [{OPERATION: /NESTED LOOP/}, {OPERATION: /FILTER/}], USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.ID, USERNAME: x.USERNAME, record_id:x.record_id});});db.@tmp@.find().forEach(function(x){db.sqlplan.find({SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.PARENT_ID,USERNAME: x.USERNAME, record_id: x.record_id}).forEach(function(y){db.@tmp1@.save({SQL_ID: y.SQL_ID, PLAN_HASH_VALUE: y.PLAN_HASH_VALUE, OBJECT_NAME: y.OBJECT_NAME, ID: y.ID, PARENT_ID: y.PARENT_ID, OPERATION: y.OPERATION, OPTIONS: y.OPTIONS, USERNAME: y.USERNAME, record_id: y.record_id})});});db.@tmp@.drop();db.@tmp1@.aggregate([{$group:{_id:{PARENT_ID:\"$PARENT_ID\",SQL_ID:\"$SQL_ID\",PLAN_HASH_VALUE:\"$PLAN_HASH_VALUE\"},MAXID: {$max:\"$ID\"}}}]).forEach(function(z){db.sqlplan.find({SQL_ID:z._id.SQL_ID,PLAN_HASH_VALUE:z._id.PLAN_HASH_VALUE,$and:[{ID:z.MAXID},{ID:{$ne:2}}],\"USERNAME\":\"@username@\", record_id: '@record_id@', OPERATION:\"TABLE ACCESS\",OPTIONS:\"FULL\"}).forEach(function(y){if(db.obj_tab_info.findOne({OWNER: y.OBJECT_OWNER, IPADDR: '@ip_addr@', SID: '@sid@', TABLE_NAME: y.OBJECT_NAME,$or: [{\"NUM_ROWS\":{$gt:@table_row_num@}},{\"PHY_SIZE(MB)\":{$gt:@table_phy_size@}}]}))db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"OBJECT_NAME\":y.OBJECT_NAME,\"ID\":y.ID,\"COST\":y.COST,\"COUNT\":\"\"})});})",
    'SQL_TABLE_FULL_SCAN': "db.@sql@.find({OPERATION: 'TABLE ACCESS', OPTIONS: 'FULL', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({SQL_ID:x.SQL_ID, PLAN_HASH_VALUE:x.PLAN_HASH_VALUE, OBJECT_NAME:x.OBJECT_NAME, ID:x.ID, COST:x.COST, COUNT:''});})"
}

OTHER_SQL = [
    "ADMINISTER",
    "EXPLAIN",
    "FLASHBACK",
    "LOCK",
    "ALTER",
    "ANALYZE",
    "DROP",
    "COMMENT",
    "RENAME",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "REVOKE",
    "GRANT",
    "AUDIT",
    "NOAUDIT",
    "PURGE",
    "CALL",
    "TRUNCATE"
]

CREATE_DBA_SQL = [
    ["CREATE", "", "", "PROFILE"],
    ["CREATE", "", "", "CONTROLFILE"],
    ["CREATE", "", "", "DATABASE"],
    ["CREATE", "", "", "DISKGROUP"],
    ["CREATE", "", "", "FLASHBACK\s+ARCHIVE"],
    ["CREATE", "", "", "LOCKDOWN\s+PROFILE"],
    ["CREATE", "", "", "OUTLINE"],
    ["CREATE", "", "", "PFILE"],
    ["CREATE", "", "", "PLUGGABLE\s+DATABASE"],
    ["CREATE", "", "", "RESTORE\s+POINT"],
    ["CREATE", "", "", "ROLE"],
    ["CREATE", "", "", "ROLLBACK\s+SEGMENT"],
    ["CREATE", "", "", "SPFILE"],
    ["CREATE", "", "", "TABLESPACE"],
    ["CREATE", "", "", "TABLESPACE\s+SET"],
]

CREATE_APP_SQL = [
    # declare 特殊处理
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "FUNCTION", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PACKAGE", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PACKAGE BODY", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PROCEDURE", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TRIGGER", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TYPE\s+BODY", "\/"],
    ["CREATE", "OR\s+REPLACE", "", "", "ANALYTIC\s+VIEW", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "ATTRIBUTE\s+DIMENSION", ";"],
    ["CREATE", "", "", "", "AUDIT\s+POLICY", ";"],
    ["CREATE", "", "", "", "CLUSTER", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "CONTEXT", ";"],
    ["CREATE", "SHARED|PUBLIC", "", "", "DATABASE\s+LINK", ";"],
    ["CREATE", "", "", "", "DIMENSION", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "DIRECTORY", ";"],
    ["CREATE", "", "", "", "EDITION", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "HIERARCHY", ";"],
    ["CREATE", "UNIQUE|BITMAP", "", "", "INDEX", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "INDEXTYPE", ";"],
    ["CREATE", "", "", "", "INMEMORY\s+JOIN\s+GROUP", ";"],
    ["CREATE", "OR\s+REPLACE", "AND\s+RESOLVE|AND\s+COMPILE", "NOFORCE", "JAVA", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "LIBRARY", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+VIEW", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+VIEW\s+LOG", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+ZONEMAP", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "OPERATOR", ";"],
    ["CREATE", "", "", "", "SCHEMA", ";"],
    ["CREATE", "", "", "", "SEQUENCE", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "PUBLIC", "SYNONYM", ";"],
    ["CREATE", "GLOBAL TEMPORARY|PRIVATE TEMPORARY|SHARDED|DUPLICATED", "", "", "TABLE", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TYPE", ";"],
    ["CREATE", "", "", "", "USER", ";"],
    ["CREATE", "OR\s+REPLACE", "NO FORCE|FORCE", "EDITIONING|EDITIONABLE EDITIONING|NONEDITIONABLE", "VIEW", ";"],
]

SQLPLUS_SQL = [
    "ACC",
    "ACCEPT",
    "A",
    "APPEND",
    "ARCHIVE LOG",
    "ATTR",
    "ATTRIBUTE",
    "BRE",
    "BREAK",
    "BTI",
    "BTITLE",
    "C",
    "CHANGE",
    "CL",
    "CLEAR",
    "COL",
    "COLUMN",
    "COMP",
    "COMPUTE",
    "CONN",
    "CONNECT",
    "DEF",
    "DEFINE",
    "DEL",
    "DESC",
    "DESCRIBE",
    "DISC",
    "DISCONNECT",
    "ED",
    "EDIT",
    "EXEC",
    "EXECUTE",
    "EXIT",
    "GET",
    "HELP",
    "HIST",
    "HISTORY",
    "HO",
    "HOST",
    "I",
    "INPUT",
    "L",
    "LIST",
    "PASS",
    "PASSWORD",
    "PAU",
    "PAUSE",
    "PRINT",
    "PRO",
    "PROMPT",
    "QUIT",
    "RECOVER",
    "REM",
    "REMARK",
    "REPF",
    "REPFOOTER",
    "REPH",
    "REPHEADER",
    "R",
    "RUN",
    "SAV",
    "SAVE",
    "SHO",
    "SHOW",
    "SHUTDOWN",
    "SPO",
    "SPOOL",
    "STA",
    "START",
    "STARTUP",
    "STORE",
    "TIMI",
    "TIMING",
    "TTI",
    "TTITLE",
    "UNDEF",
    "UNDEFINE",
    "VAR",
    "VARIABLE",
    "WHENEVER\s+OSERROR",
    "WHENEVER\s+SQLERROR",
    "XQUERY"
]


def parse_sql_file(sql_contents, sql_keyword):
    """读取sql文件"""

    def get_procedures_end_with_slash(sql_contents):
        cate = ["declare",
                "create\s+(?:or\s+replace\s+)?(?:EDITIONABLE|NONEDITIONABLE\s+)?(?:FUNCTION|PACKAGE|PACKAGE BODY|PROCEDURE|TRIGGER|TYPE BODY)"]
        re_str = f"\n(\s*set\s+\w+\s+\w+)|\n\s*((?:{'|'.join(cate)})[\s\S]*?end;[\s\S]*?\/)|\n\s*((?:{'|'.join(SQLPLUS_SQL)})(?:\s+.*?)?)\n|\n\s*(@@?.*?)\n"
        procedures = [''.join(x) for x in re.findall(re_str, sql_contents, re.I)]
        return procedures

    def is_annotation(sql):
        multi_annotation = re.findall("\/\*[\s\S]*?\*\/", sql, re.I)
        for anno in multi_annotation:
            sql = sql.replace(anno, "")
        return all([annotation_condition(x) for x in sql.split("\n")])

    def annotation_condition(sql):
        sql = sql.strip()
        if not sql:
            return True
        if re.match("^\s*(remark|rem|--|\/\*)\s+", sql, re.I):
            return True
        if re.match('\s*--.*?', sql, re.I):
            return True
        if re.match('\/\* +.*?\*\/', sql, re.I):
            return True
        if re.match('\/\*[^+]{2}[\s\S]*?\*\/\n', sql, re.I):
            return True
        return False


    # sql_keyword doesn't used.

    procedures = get_procedures_end_with_slash(sql_contents)
    for procedure in procedures:
        sql_contents = sql_contents.replace(procedure, "|||||")

    sql_contents = [x.strip(' ') for x in sql_contents.split("|||||")]

    sql_list = []
    for index, content in enumerate(sql_contents):
        sql_list += [a for b in [sqlparse.split(x) for x in content.split(';')] for a in b]
        if index < len(procedures) and procedures[index].strip():
            sql_list.append(procedures[index].strip())

    sql_list = [sql for sql in sql_list if sql]

    new_sql_list = []
    annotation_sql = ""
    for sql in sql_list:

        if is_annotation(sql):
            annotation_sql += sql
        else:
            new_sql_list.append(
                (annotation_sql + "\n" + sql).lstrip().replace('\n\n', '\n').replace('\n', '<br/>').replace("\"", "'"))
            annotation_sql = ""

    return new_sql_list


@timing
def get_sql_id_stats(cmdb_id, etl_date_gte=None) -> dict:
    """
    计算sql文本的统计信息
    :param cmdb_id:
    :param etl_date_gte: etl时间晚于
    :return: {sql_id: {}}
    """
    # TODO use cache!
    # TODO use bulk aggregation instead of aggregate one by one!

    match_case = {
        'cmdb_id': cmdb_id,
        # 'etl_date': {"$gte": , "$lt": }
    }
    if etl_date_gte:
        match_case["etl_date"] = {}
        match_case["etl_date"]["$gte"] = etl_date_gte
    to_aggregate = [
        {
            "$match": match_case
        },
        {
            "$group": {
                "_id": "$SQL_ID",
                "first_appearance": {"$min": "$ETL_DATE"},
                "last_appearance": {"$max": "$ETL_DATE"},
                "count": {"$sum": 1}
            }
        }
    ]
    ret = SQLText.objects.aggregate(*to_aggregate)
    return {i["_id"]: i for i in ret}


@timing
def get_sql_plan_stats(cmdb_id, etl_date_gte=None) -> dict:
    """
    计算sql计划的统计信息
    :param cmdb_id:
    :param etl_date_gte: etl时间晚于
    :return: {plan_hash_value: {}}
    """
    # TODO use cache!
    # TODO use bulk aggregation instead of aggregate one by one!
    match_case = {
        'cmdb_id': cmdb_id,
        # 'etl_date': {"$gte": , "$lt": }
    }
    if etl_date_gte:
        match_case["etl_date"] = {}
        match_case["etl_date"]["$gte"] = etl_date_gte
    to_aggregate = [
        {
            "$match": match_case
        },
        {
            "$group": {
                "_id": "$PLAN_HASH_VALUE",
                "first_appearance": {"$min": "$ETL_DATE"},
                "last_appearance": {"$max": "$ETL_DATE"},
            }
        }
    ]
    ret = MSQLPlan.objects.aggregate(*to_aggregate)
    return {i["_id"]: i for i in ret}


@timing
def get_sql_id_sqlstat_dict(record_id: Union[tuple, list, str]) -> dict:
    """
    获取最近捕获的sql文本统计信息(在给定的record_id中)
    :param record_id: 可传单个或者list
    :return: {sql_id: {stats, ...}, ...}
    """
    if not isinstance(record_id, (list, tuple)):
        if isinstance(record_id, str):
            record_id = [record_id]
        else:
            assert 0
    keys = ["sql_id", "elapsed_time_delta", "executions_delta", "schema"]
    return {i[0]: dict(zip(keys[1:], i[1:])) for i in
            SQLStat.objects(record_id__in=record_id).order_by("-etl_date").values_list(*keys)}


@timing
def get_risk_sql_list(session,
                      cmdb_id: str,
                      date_range: (date, date),
                      schema_name: str = None,
                      rule_type: str = "ALL",
                      risk_sql_rule_id: list = (),
                      sort_by: str = "last",
                      enable_white_list: bool = True,
                      current_user: str = None,
                      sql_id_only: bool = False,
                      sqltext_stats: bool = True,
                      **kwargs
                      ) -> Union[dict, set]:
    """
    获取风险SQL列表
    :param session:
    :param cmdb_id:
    :param date_range:
    :param schema_name:
    :param rule_type:
    :param risk_sql_rule_id:
    :param sort_by:
    :param enable_white_list:
    :param current_user: 需要过滤登录用户
    :param sql_id_only: 仅仅返回sql_id的set
    :param sqltext_stats: 返回是否需要包含sqltext的统计信息（首末出现时间）
    :param kwargs: 多余的参数，会被收集到这里，并且会提示
    :return:
    """
    # 因为参数过多，加个判断。
    date_start, date_end = date_range
    assert sort_by in ("last", "average")
    assert rule_type in ["ALL"] + const.ALL_RULE_TYPES_FOR_SQL_RULE
    if kwargs:
        print(f"got extra useless kwargs: {kwargs}")

    cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
    risk_rule_q = session.query(RiskSQLRule)
    result_q = Results.objects(cmdb_id=cmdb_id)
    if schema_name:
        if schema_name not in \
                cmdb_utils.get_current_schema(session, current_user, cmdb_id):
            raise Exception(f"无法在编号为{cmdb_id}的数据库中"
                            f"操作名为{schema_name}的schema。")
        result_q = result_q.filter(schema_name=schema_name)
    if rule_type == "ALL":
        rule_type: list = const.ALL_RULE_TYPES_FOR_SQL_RULE
    else:
        rule_type: list = [rule_type]
    risk_rule_q = risk_rule_q.filter(RiskSQLRule.rule_type.in_(rule_type))
    result_q = result_q.filter(rule_type__in=rule_type)

    if risk_sql_rule_id:
        risk_rule_q = risk_rule_q.filter(RiskSQLRule.risk_sql_rule_id.
                                         in_(risk_sql_rule_id))
    if date_start:
        result_q = result_q.filter(create_date__gte=date_start)
    if date_end:
        result_q = result_q.filter(create_date__lte=date_end)
    risky_rules = Rule.filter_enabled(
        rule_name__in=[i[0] for i in risk_rule_q.with_entities(RiskSQLRule.rule_name)],
        db_model=cmdb.db_model,
        db_type=const.DB_ORACLE
    )
    risk_rules_dict = rule_utils.get_risk_rules_dict(session)
    risky_rule_name_object_dict = {risky_rule.rule_name:
                                       risky_rule for risky_rule in risky_rules.all()}
    if not risky_rule_name_object_dict:
        raise const.NoRiskRuleSetException
    get_risk_sql_list.tik(f"risk sql rule count: {len(risky_rule_name_object_dict)}")

    # 过滤出包含风险SQL规则结果的result
    Qs = None
    for risky_rule_name in risky_rule_name_object_dict.keys():
        if not Qs:
            Qs = Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
        else:
            Qs = Qs | Q(**{f"{risky_rule_name}__sqls__nin": [None, []]})
    if Qs:
        result_q = result_q.filter(Qs)
    get_risk_sql_list.tik(f"result count: {result_q.count()}")

    rst = []  # 详细信息的返回结果
    rst_sql_id_set = set()  # 统计sql_id防止重复

    if not sql_id_only:
        # ====== 如果仅统计sql_id，以下信息不需要 ======
        sql_text_stats = {}
        if sqltext_stats:
            sql_text_stats = get_sql_id_stats(cmdb_id)
        # 统计全部搜索到的result的record_id内的全部sql_id的最近一次运行的统计信息
        last_sql_id_sqlstat_dict = get_sql_id_sqlstat_dict(list(result_q.distinct("record_id")))

    for result in result_q:

        # result具有可变字段，具体结构请参阅models.mongo.results

        for risky_rule_name, risky_rule_object in risky_rule_name_object_dict.items():
            risk_rule_object = risk_rules_dict[risky_rule_object.get_3_key()]

            # risky_rule_object is a record of Rule from mongodb

            # risk_rule_object is a record of RiskSQLRule from oracle

            if not getattr(result, risky_rule_name, None):
                continue  # 规则key不存在，或者值直接是个空dict，则跳过
            if not getattr(result, risky_rule_name).get("sqls", None):
                # 规则key下的sqls不存在，或者值直接是个空list，则跳过
                # e.g. {"XXX_RULE_NAME": {"scores": 0.0}}  # 无sqls
                # e.g. {"XXX_RULE_NAME": {"sqls": [], "scores": 0.0}}
                continue

            sqls = getattr(result, risky_rule_name)["sqls"]

            if sql_id_only:
                rst_sql_id_set.update([i["sql_id"] for i in sqls])
                continue

            # NOTICE: 以下代码必须保证sql_id_only == False

            for sql_text_dict in sqls:
                sql_id = sql_text_dict["sql_id"]
                if sql_id in rst_sql_id_set:
                    continue
                sqlstat_dict = last_sql_id_sqlstat_dict[sql_id]
                execution_time_cost_sum = round(sqlstat_dict["elapsed_time_delta"], 2)  # in ms
                execution_times = sqlstat_dict.get('executions_delta', 0)
                execution_time_cost_on_average = 0
                if execution_times:
                    execution_time_cost_on_average = round(execution_time_cost_sum / execution_times, 2)
                r = {
                    "sql_id": sql_id,
                    "schema": sqlstat_dict["schema"],
                    "sql_text": sql_text_dict["sql_text"],
                    "rule_desc": risky_rule_object.rule_desc,
                    "severity": risk_rule_object.severity,
                    "similar_sql_num": 1,  # sql_text_stats[sql_id]["count"],  # TODO 这是啥？
                    "execution_time_cost_sum": execution_time_cost_sum,
                    "execution_times": execution_times,
                    "execution_time_cost_on_average": execution_time_cost_on_average,
                    "risk_sql_rule_id": risk_rule_object.risk_sql_rule_id,
                }
                if sqltext_stats:
                    r.update({
                        "first_appearance": dt_to_str(sql_text_stats[sql_id]['first_appearance']),
                        "last_appearance": dt_to_str(sql_text_stats[sql_id]['last_appearance']),
                    })
                rst.append(r)
                rst_sql_id_set.add(sql_id)
    if sql_id_only:
        return rst_sql_id_set
    if sort_by == "sum":
        rst = sorted(rst, key=lambda x: x["execution_time_cost_sum"], reverse=True)
    elif sort_by == "average":
        rst = sorted(rst, key=lambda x: x["execution_time_cost_on_average"], reverse=True)
    return rst
